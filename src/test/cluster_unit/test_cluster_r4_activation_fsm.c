/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_activation_fsm.c
 *	  Exact closed-transition, ACK and dormant-admission tests for R4 D13.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_epoch_ballot.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_control_root.h" /* bit22 (G3 accessor test) */
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_replacement_wire.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_block0_current.h"

extern bool cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out);
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/shmem.h"

#define TEST_SEMANTIC_GATE_SHMEM_BYTES 1104
#define TEST_SEMANTIC_UTILITY_MAILBOX_BYTES 80
#define TEST_SEMANTIC_ACK_TABLE_BYTES 16496
#define TEST_SEMANTIC_PGRD_SNAPSHOT_BYTES 528
/* RF-ROOT P7 (contract §B): ClusterR4Bit22CutoverLatchShmem = u32+u32+u64+u64 */
#define TEST_SEMANTIC_BIT22_LATCH_BYTES 24
/* RF-ROOT P9 verification: ClusterR4Bit22SourceCloseShmem = u32+u32+u64+u64 */
#define TEST_SEMANTIC_BIT22_SOURCE_CLOSE_BYTES 24
/* RF-ROOT P7 (contract): ClusterR4Bit22CutoverSeamShmem */
#define TEST_SEMANTIC_BIT22_SEAM_BYTES 216
#define TEST_GATE_SEQ_OFFSET 552
#define TEST_GATE_ACTIVE_BITS_OFFSET 560
#define TEST_GATE_RECORD_GENERATION_OFFSET 568
#define TEST_GATE_FORMATION_EPOCH_OFFSET 576
#define TEST_GATE_CLOSED_OFFSET 584
#define TEST_GATE_INFLIGHT_OFFSET 588

typedef union TestSemanticShmemStorage {
	pg_atomic_uint64 align;
	uint8 bytes[TEST_SEMANTIC_GATE_SHMEM_BYTES];
} TestSemanticShmemStorage;

typedef union TestSemanticUtilityMailboxStorage {
	pg_atomic_uint64 align;
	uint8 bytes[TEST_SEMANTIC_UTILITY_MAILBOX_BYTES];
} TestSemanticUtilityMailboxStorage;

typedef union TestSemanticAckTableStorage {
	pg_atomic_uint64 align;
	uint8 bytes[TEST_SEMANTIC_ACK_TABLE_BYTES];
} TestSemanticAckTableStorage;

typedef union TestSemanticPgrdSnapshotStorage {
	pg_atomic_uint64 align;
	uint8 bytes[TEST_SEMANTIC_PGRD_SNAPSHOT_BYTES];
} TestSemanticPgrdSnapshotStorage;

typedef union TestSemanticBit22LatchStorage {
	pg_atomic_uint64 align;
	uint8 bytes[TEST_SEMANTIC_BIT22_LATCH_BYTES];
} TestSemanticBit22LatchStorage;

typedef union TestSemanticBit22SeamStorage {
	pg_atomic_uint64 align;
	uint8 bytes[TEST_SEMANTIC_BIT22_SEAM_BYTES];
} TestSemanticBit22SeamStorage;

static TestSemanticShmemStorage test_semantic_shmem;
static TestSemanticUtilityMailboxStorage test_semantic_utility_mailbox;
static TestSemanticAckTableStorage test_semantic_ack_table;
static TestSemanticPgrdSnapshotStorage test_semantic_pgrd_snapshot;
static TestSemanticBit22LatchStorage test_semantic_bit22_latch;
typedef struct TestSemanticSourceCloseStorage {
	uint8 bytes[TEST_SEMANTIC_BIT22_SOURCE_CLOSE_BYTES];
} TestSemanticSourceCloseStorage;
static TestSemanticSourceCloseStorage test_semantic_source_close;
static TestSemanticBit22SeamStorage test_semantic_bit22_seam;
static pg_atomic_uint32 test_semantic_clean_start;
static bool test_shmem_found;
static bool test_utility_mailbox_found;
static bool test_ack_table_found;
static bool test_pgrd_snapshot_found;
static bool test_bit22_latch_found;
static bool test_bit22_seam_found;
static bool test_source_close_found;
static Size test_source_close_requested_size;
static Size test_shmem_requested_size;
static Size test_utility_mailbox_requested_size;
static Size test_ack_table_requested_size;
static Size test_pgrd_snapshot_requested_size;
static Size test_bit22_latch_requested_size;
static Size test_bit22_seam_requested_size;
static pg_on_exit_callback test_exit_callback;
static Datum test_exit_callback_arg;
static int test_exit_registration_count;
static uint64 test_current_epoch = 7;
static int test_read_barrier_count;
static int test_advance_epoch_on_read_barrier;
static bool test_peer_capability_matches;
static int test_peer_capability_match_calls;
static int32 test_peer_capability_match_peer;
static uint32 test_peer_capability_match_caps;
static uint32 test_peer_capability_match_generation;
static bool test_peer_capability_word_sample_ok;
static uint32 test_peer_capability_word;
static uint32 test_peer_capability_generation;
static int test_peer_capability_sample_calls[CLUSTER_MAX_NODES];
static int test_capability_missing_peer = -1;
static bool test_capability_store_missing;
static uint32 test_local_capability_word;
static bool test_ctrc_shmem_is_ready = true;
static ClusterICSendResult test_send_results[CLUSTER_MAX_NODES];
static int test_send_calls[CLUSTER_MAX_NODES];
static uint8 test_send_msg_types[CLUSTER_MAX_NODES];
static uint32 test_send_payload_lengths[CLUSTER_MAX_NODES];
static uint8 test_send_payloads[CLUSTER_MAX_NODES]
	[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
#define TEST_SEND_HISTORY_CAPACITY 16
static uint8 test_send_history_msg_types[CLUSTER_MAX_NODES]
	[TEST_SEND_HISTORY_CAPACITY];
static uint32 test_send_history_payload_lengths[CLUSTER_MAX_NODES]
	[TEST_SEND_HISTORY_CAPACITY];
static uint8 test_send_history_payloads[CLUSTER_MAX_NODES]
	[TEST_SEND_HISTORY_CAPACITY]
	[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
static int test_close_calls[CLUSTER_MAX_NODES];
static int test_phase3_observe_calls;
static ClusterReplacementPhase3HandoffItem test_phase3_observed_item;
static int test_admitted_snapshot_calls;
static bool test_admitted_snapshot_valid;
static ClusterReplacementEpisode test_admitted_snapshot_episode;
static ClusterReplacementCommitMarkerV3 test_admitted_snapshot_marker;
static int test_initial_clean_snapshot_calls;
static bool test_initial_clean_snapshot_valid;
static ClusterInitialCleanFormationSnapshot test_initial_clean_snapshot;
static uint32 test_grd_recovery_state = GRD_RECOVERY_IDLE;
static int test_membership_snapshot_calls;
static bool test_membership_snapshot_valid;
static int test_membership_snapshot_fail_at_call;
static uint64 test_membership_snapshot_lo;
static uint64 test_membership_snapshot_hi;
static uint64 test_membership_snapshot_epoch;
static int test_r4a_snapshot_calls;
static ClusterR4PrerequisiteSnapshot test_r4a_snapshot = {
	.target_node_id = -1,
};
static int test_wait_sleep_calls;
static int test_complete_after_wait_sleeps;
static uint64 test_wait_completion_request_seq;
static bool test_wait_completion_succeeded;
static ClusterLmsSharedState test_lms_state;
static bool test_lms_state_available;
static bool test_drain_request_succeeds;
static uint64 test_drain_worker_incarnation;
static int test_drain_request_calls;
static uint64 test_drain_request_generation;
static int test_lms_wakeup_calls;
static int test_lms_wakeup_worker;
static bool test_reclaim_succeeds;
static int test_reclaim_calls;
static uint64 test_reclaim_worker_incarnation;
static uint64 test_reclaim_generation;
static uint64 test_route_purge_result;
static int test_route_purge_calls;
static uint64 test_route_count;
static int test_route_count_calls;
static uint64 test_requester_count;
static int test_requester_count_calls;
static ClusterUndoSmgrRootMirrorState test_pgrd_candidate_state
	= CLUSTER_UNDO_SMGR_ROOT_MIRROR_ABSENT;
static uint8 test_pgrd_candidate[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
static int test_pgrd_candidate_read_calls;
static char test_pgrd_candidate_root_directory[MAXPGPATH];
static ClusterUndoSmgrRootMirrorState test_pgrd_publish_result
	= CLUSTER_UNDO_SMGR_ROOT_MIRROR_PUBLISHED;
static int test_pgrd_publish_calls;
static char test_pgrd_publish_root_directory[MAXPGPATH];
static uint8 test_pgrd_published[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
static int test_strong_random_calls;
static uint64 test_system_identifier;
static bool test_qvotec_in_quorum = true;
static uint64 test_qvotec_self_incarnation = UINT64_C(0x445566778899aabb);
static uint64 test_last_admitted_incarnation = UINT64_C(0x445566778899aabb);
static uint64 test_remote_admitted_incarnations[CLUSTER_MAX_NODES];
static bool test_observed_slot_valid[CLUSTER_MAX_NODES];
static uint64 test_observed_slot_incarnation[CLUSTER_MAX_NODES];
static uint64 test_observed_slot_generation[CLUSTER_MAX_NODES];
static uint64 test_observed_slot_epoch[CLUSTER_MAX_NODES];
static bool test_resource_x_gate_snapshot_valid;
static ResourceXGateSnapshot test_resource_x_gate_snapshot;
static bool test_resource_x_cutover_digest_valid;
static ResourceXReconfigToken test_resource_x_cutover_token;
static bool test_resource_x_cutover_thawed;
static uint64 test_resource_x_cutover_digest;

int MyProcPid = 101;
int cluster_node_id = 1;
char *cluster_shared_data_dir;
volatile sig_atomic_t InterruptPending = false;
volatile uint32 InterruptHoldoffCount = 0;
volatile uint32 QueryCancelHoldoffCount = 0;
volatile uint32 CritSectionCount = 0;

static bool semantic_activation_utility_mailbox_complete(
	uint64 request_seq, ClusterSemanticActivationResult result,
	uint64 feature_bit, uint64 expected_generation);
ClusterICSendResult cluster_ic_send_envelope(
	uint8 msg_type, int32 dest_node_id, const void *payload,
	uint32 payload_len);
void cluster_ic_tier1_close_peer(int32 peer_id, const char *reason);

bool
cluster_pcm_lock_resource_x_gate_snapshot(ResourceXGateSnapshot *snapshot_out)
{
	if (snapshot_out == NULL)
		return false;
	memset(snapshot_out, 0, sizeof(*snapshot_out));
	if (!test_resource_x_gate_snapshot_valid)
		return false;
	*snapshot_out = test_resource_x_gate_snapshot;
	return true;
}

bool
cluster_pcm_lock_resource_x_cutover_gate_snapshot_exact(
	ResourceXGateSnapshot *snapshot_out)
{
	if (!cluster_pcm_lock_resource_x_gate_snapshot(snapshot_out)
		|| snapshot_out->phase != RESOURCE_X_GATE_OPEN) {
		if (snapshot_out != NULL)
			memset(snapshot_out, 0, sizeof(*snapshot_out));
		return false;
	}
	return true;
}

bool
cluster_pcm_lock_resource_x_cutover_current_proof_digest_exact(
	bool thawed, ResourceXReconfigToken *token_out, uint64 *digest_out)
{
	if (token_out == NULL || digest_out == NULL)
		return false;
	memset(token_out, 0, sizeof(*token_out));
	*digest_out = 0;
	if (!test_resource_x_cutover_digest_valid
		|| thawed != test_resource_x_cutover_thawed
		|| test_resource_x_cutover_digest == 0)
		return false;
	*token_out = test_resource_x_cutover_token;
	*digest_out = test_resource_x_cutover_digest;
	return true;
}

void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	if (strcmp(name, "pgrac cluster semantic activation clean start") == 0) {
		*foundPtr = false;
		return &test_semantic_clean_start;
	}
	if (strcmp(name, "pgrac cluster semantic activation PGRD snapshot") == 0) {
		test_pgrd_snapshot_requested_size = size;
		*foundPtr = test_pgrd_snapshot_found;
		return test_semantic_pgrd_snapshot.bytes;
	}
	if (strcmp(name, "pgrac cluster semantic activation ACK table") == 0) {
		test_ack_table_requested_size = size;
		*foundPtr = test_ack_table_found;
		return test_semantic_ack_table.bytes;
	}
	if (strcmp(name, "pgrac cluster semantic activation utility mailbox") == 0) {
		test_utility_mailbox_requested_size = size;
		*foundPtr = test_utility_mailbox_found;
		return test_semantic_utility_mailbox.bytes;
	}
	if (strcmp(name, "pgrac cluster r4 bit22 cutover latch") == 0) {
		test_bit22_latch_requested_size = size;
		*foundPtr = test_bit22_latch_found;
		return test_semantic_bit22_latch.bytes;
	}
	if (strcmp(name, "pgrac cluster r4 bit22 cutover seam") == 0) {
		test_bit22_seam_requested_size = size;
		*foundPtr = test_bit22_seam_found;
		return test_semantic_bit22_seam.bytes;
	}
	if (strcmp(name, "pgrac cluster r4 bit22 source close") == 0) {
		test_source_close_requested_size = size;
		*foundPtr = test_source_close_found;
		return test_semantic_source_close.bytes;
	}
	test_shmem_requested_size = size;
	*foundPtr = test_shmem_found;
	return test_semantic_shmem.bytes;
}

uint64
cluster_epoch_get_current(void)
{
	return test_current_epoch;
}

/* implementation (contract §C / follow-up contract ②): the runtime census self-check
 * stub — the bit22 latch apply consults it; RED (a KNOWN-DEFERRED site
 * still linked) refuses the flip, so the cutover round must close every
 * deferred site before the latch opens.  This binary does not link
 * cluster_wal_state.o, hence the stub. */
static bool ut_r4fsm_census_ok = true;

bool
cluster_wal_state_correctness_census_ok(void)
{
	return ut_r4fsm_census_ok;
}

bool
cluster_ctrc_shmem_ready(void)
{
	return test_ctrc_shmem_is_ready;
}

/* RF-ROOT P7 (contract, step ②): the root activation stub — this binary
 * does not link cluster_control_root.o.  The advance's executor path is
 * exercised via ut_activate_result (OK advances; non-OK stays PREPARED). */
static ClusterControlRootResult ut_activate_result
	= CLUSTER_CONTROL_ROOT_OK_PRIMARY;
static int ut_activate_calls = 0;

ClusterControlRootResult
cluster_control_root_activate_prepared(
	const ClusterControlRootFileToken *expected_token pg_attribute_unused(),
	const uint8 expected_round_sha256[32] pg_attribute_unused(),
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused(),
	ClusterControlRootFileToken *out_token)
{
	ut_activate_calls++;
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	return ut_activate_result;
}

/* RF-ROOT P9 verification: bootstrap_validate_active_round stub — the
 * member-side COMMIT_APPLIED verification of the ACTIVE root. */
static ClusterControlRootResult ut_bootstrap_validate_result
	= CLUSTER_CONTROL_ROOT_OK_PRIMARY;
static int ut_bootstrap_validate_calls = 0;

ClusterControlRootResult
cluster_control_root_bootstrap_validate_active_round(
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused(),
	ClusterControlRootFileToken *token)
{
	ut_bootstrap_validate_calls++;
	if (token != NULL)
		memset(token, 0, sizeof(*token));
	return ut_bootstrap_validate_result;
}

/* RF-ROOT P7 (contract step ④c): create_prepared / round_sha256 stubs — this
 * binary does not link cluster_control_root.o. */
static ClusterControlRootResult ut_create_result
	= CLUSTER_CONTROL_ROOT_OK_PRIMARY;
static int ut_create_calls = 0;

ClusterControlRootResult
cluster_control_root_create_prepared(
	const ClusterControlRootMigrationImage *image pg_attribute_unused(),
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused(),
	ClusterControlRootFileToken *out_token)
{
	ut_create_calls++;
	if (out_token != NULL) {
		memset(out_token, 0, sizeof(*out_token));
		if (ut_create_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			out_token->file_txn_seq = 1;
	}
	return ut_create_result;
}

/* RF-ROOT P9 verification (follow-up): the SQL entry (step ④e) references
 * superuser(); this binary does not link the backend superuser machinery. */
bool
superuser(void)
{
	return true;
}

ClusterControlRootResult
cluster_control_root_build_migration_image(
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused(),
	ClusterControlRootMigrationImage *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

bool
cluster_control_root_round_sha256(
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused(),
	uint8 out_sha[PG_SHA256_DIGEST_LENGTH])
{
	if (out_sha != NULL)
		memset(out_sha, 0x11, PG_SHA256_DIGEST_LENGTH);
	return true;
}

/* RF-ROOT P9 verification (B′): the member-side bit22 cutover path
 * binds the ACTIVE root via
 * cluster_control_root_bootstrap_validate_active_round_fields; this binary
 * does not link cluster_control_root.o.  Configurable stub — the OPEN_PROOF
 * reconstruction tests drive both the GREEN binding and the RED identity
 * refusal. */
static ClusterControlRootResult test_r4fsm_root_validate_result
	= CLUSTER_CONTROL_ROOT_OK_PRIMARY;

ClusterControlRootResult
cluster_control_root_bootstrap_validate_active_round_fields(
	uint64 transition_epoch pg_attribute_unused(),
	uint64 prepare_generation pg_attribute_unused(),
	uint64 source_feature_bitmap pg_attribute_unused(),
	uint64 target_feature_bitmap pg_attribute_unused())
{
	return test_r4fsm_root_validate_result;
}

/* R4 cutover contract (verified implementation): the OPEN_PROOF reconstruction reads
 * the durable majority-selected semantic-activation record through
 * cluster_qvotec_bootstrap_read_semantic_activation; this binary does not
 * link cluster_qvotec.o.  Configurable stub. */
static ClusterSemanticActivationResult test_r4fsm_bootstrap_read_result
	= CLUSTER_SEMANTIC_ACTIVATION_OK;
static bool test_r4fsm_bootstrap_read_ok;
static bool test_r4fsm_bootstrap_implicit_open;
static bool test_restart_unclean;
static bool test_restart_in_recovery;
static uint8 test_r4fsm_bootstrap_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

ClusterSemanticActivationResult
cluster_qvotec_bootstrap_read_semantic_activation(
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	bool *implicit_open)
{
	if (selected == NULL || implicit_open == NULL)
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
	*implicit_open = false;
	if (test_r4fsm_bootstrap_read_result != CLUSTER_SEMANTIC_ACTIVATION_OK)
		return test_r4fsm_bootstrap_read_result;
	if (!test_r4fsm_bootstrap_read_ok)
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
	memcpy(selected, test_r4fsm_bootstrap_bytes,
		   CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
	*implicit_open = test_r4fsm_bootstrap_implicit_open;
	return CLUSTER_SEMANTIC_ACTIVATION_OK;
}

bool
cluster_qvotec_in_quorum(void)
{
	return test_qvotec_in_quorum;
}

bool
cluster_qvotec_prior_unclean_death(void)
{
	return test_restart_unclean;
}

bool
RecoveryInProgress(void)
{
	return test_restart_in_recovery;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	return test_qvotec_self_incarnation;
}

uint64
cluster_membership_get_last_admitted_incarnation(int32 node_id)
{
	if (node_id == cluster_node_id)
		return test_last_admitted_incarnation;
	return node_id >= 0 && node_id < CLUSTER_MAX_NODES
			   ? test_remote_admitted_incarnations[node_id]
			   : 0;
}

void *
palloc(Size size)
{
	return malloc(size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

uint64
GetSystemIdentifier(void)
{
	return test_system_identifier;
}

bool
pg_strong_random(void *buf, size_t len)
{
	test_strong_random_calls++;
	if (buf == NULL)
		return false;
	memset(buf, 0xa6, len);
	return true;
}

void
on_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	test_exit_callback = function;
	test_exit_callback_arg = arg;
	test_exit_registration_count++;
}

void
ProcessInterrupts(void)
{}

static void
test_pg_usleep(long microsec pg_attribute_unused())
{
	test_wait_sleep_calls++;
	if (test_complete_after_wait_sleeps > 0
		&& test_wait_sleep_calls == test_complete_after_wait_sleeps)
		test_wait_completion_succeeded
			= semantic_activation_utility_mailbox_complete(
				test_wait_completion_request_seq,
				CLUSTER_SEMANTIC_ACTIVATION_OK, 0, 0);
}

bool
cluster_sf_peer_capability_generation_matches(int32 peer_id, uint32 required_capabilities,
									  uint32 expected_generation)
{
	test_peer_capability_match_calls++;
	test_peer_capability_match_peer = peer_id;
	test_peer_capability_match_caps = required_capabilities;
	test_peer_capability_match_generation = expected_generation;
	return test_peer_capability_matches && peer_id != test_capability_missing_peer;
}

bool
cluster_sf_peer_capability_record_snapshot(int32 peer_id, ClusterSfPeerCap *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || test_capability_store_missing || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return false;
	test_peer_capability_sample_calls[peer_id]++;
	out->valid = test_peer_capability_word_sample_ok && peer_id != test_capability_missing_peer;
	out->bits = test_peer_capability_word;
	out->generation = test_peer_capability_generation;
	return true;
}

bool
cluster_sf_peer_capability_word_sample(int32 peer_id,
									  uint32 required_capabilities,
									  uint32 *capability_word_out,
									  uint32 *generation_out)
{
	if (capability_word_out != NULL)
		*capability_word_out = 0;
	if (generation_out != NULL)
		*generation_out = 0;
	if (peer_id >= 0 && peer_id < CLUSTER_MAX_NODES)
		test_peer_capability_sample_calls[peer_id]++;
	if (!test_peer_capability_word_sample_ok || peer_id == test_capability_missing_peer
		|| peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || required_capabilities == 0
		|| (test_peer_capability_word & required_capabilities) != required_capabilities)
		return false;
	if (capability_word_out != NULL)
		*capability_word_out = test_peer_capability_word;
	if (generation_out != NULL)
		*generation_out = test_peer_capability_generation;
	return true;
}

uint32
cluster_ic_local_capability_word(void)
{
	return test_local_capability_word;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id,
						 const void *payload, uint32 payload_len)
{
	int history_index;

	if (dest_node_id < 0 || dest_node_id >= CLUSTER_MAX_NODES
		|| payload == NULL
		|| payload_len != CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES)
		return CLUSTER_IC_SEND_HARD_ERROR;
	history_index = test_send_calls[dest_node_id];
	if (history_index < TEST_SEND_HISTORY_CAPACITY) {
		test_send_history_msg_types[dest_node_id][history_index] = msg_type;
		test_send_history_payload_lengths[dest_node_id][history_index]
			= payload_len;
		memcpy(test_send_history_payloads[dest_node_id][history_index],
			   payload, payload_len);
	}
	test_send_calls[dest_node_id]++;
	test_send_msg_types[dest_node_id] = msg_type;
	test_send_payload_lengths[dest_node_id] = payload_len;
	memcpy(test_send_payloads[dest_node_id], payload, payload_len);
	return test_send_results[dest_node_id];
}

void
cluster_ic_tier1_close_peer(int32 peer_id,
							const char *reason pg_attribute_unused())
{
	if (peer_id >= 0 && peer_id < CLUSTER_MAX_NODES)
		test_close_calls[peer_id]++;
}

int
cluster_membership_member_count(void)
{
	return 4;
}

bool
cluster_membership_is_member(int32 node_id)
{
	return node_id >= 0 && node_id < 4;
}

ClusterMembershipState
cluster_membership_get_state(int32 node_id)
{
	return cluster_membership_is_member(node_id)
			   ? CLUSTER_MEMBER_MEMBER
			   : CLUSTER_MEMBER_REMOVED;
}

bool
cluster_reconfig_get_observed_slot(int32 node_id, uint64 *incarnation,
								   uint64 *generation)
{
	if (incarnation != NULL)
		*incarnation = 0;
	if (generation != NULL)
		*generation = 0;
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES
		|| incarnation == NULL || generation == NULL
		|| !test_observed_slot_valid[node_id])
		return false;
	*incarnation = test_observed_slot_incarnation[node_id];
	*generation = test_observed_slot_generation[node_id];
	return true;
}

uint64
cluster_reconfig_get_observed_epoch(int32 node_id)
{
	return node_id >= 0 && node_id < CLUSTER_MAX_NODES
			   ? test_observed_slot_epoch[node_id]
			   : 0;
}

bool
cluster_reconfig_lmon_observe_replacement_ready(
	const ClusterReplacementPhase3HandoffItem *item)
{
	test_phase3_observe_calls++;
	if (item != NULL)
		test_phase3_observed_item = *item;
	return item != NULL;
}

bool
cluster_reconfig_lmon_snapshot_replacement_admitted(
	ClusterReplacementEpisode *out_episode,
	ClusterReplacementCommitMarkerV3 *out_marker)
{
	test_admitted_snapshot_calls++;
	if (!test_admitted_snapshot_valid || out_episode == NULL || out_marker == NULL)
		return false;
	*out_episode = test_admitted_snapshot_episode;
	*out_marker = test_admitted_snapshot_marker;
	return true;
}

bool
cluster_reconfig_snapshot_initial_clean_formation(
	ClusterInitialCleanFormationSnapshot *out)
{
	test_initial_clean_snapshot_calls++;
	if (!test_initial_clean_snapshot_valid || out == NULL)
		return false;
	*out = test_initial_clean_snapshot;
	return true;
}

uint32
cluster_grd_recovery_state_value(void)
{
	return test_grd_recovery_state;
}

bool
cluster_reconfig_lmon_snapshot_admitted_membership(
	uint64 *out_members_lo, uint64 *out_members_hi,
	uint64 *out_formation_epoch)
{
	test_membership_snapshot_calls++;
	if (!test_membership_snapshot_valid
		|| test_membership_snapshot_calls == test_membership_snapshot_fail_at_call
		|| out_members_lo == NULL || out_members_hi == NULL || out_formation_epoch == NULL)
		return false;
	*out_members_lo = test_membership_snapshot_lo;
	*out_members_hi = test_membership_snapshot_hi;
	*out_formation_epoch = test_membership_snapshot_epoch;
	return true;
}

static ClusterR4PrerequisiteSnapshot
test_r4a_prerequisite_snapshot(void)
{
	test_r4a_snapshot_calls++;
	return test_r4a_snapshot;
}

/* Satisfy the independently linked block0 facade; this fixture overrides the
 * activation module's prerequisite call with test_r4a_prerequisite_snapshot. */
ClusterR4PrerequisiteSnapshot
cluster_reconfig_r4_prerequisite_snapshot(void)
{
	return test_r4a_snapshot;
}

bool
cluster_reconfig_r4_publish_ready(
	const ClusterR4PrerequisiteSnapshot *expected pg_attribute_unused())
{
	return false;
}

ClusterLmsSharedState *
cluster_lms_shared_state(void)
{
	return test_lms_state_available ? &test_lms_state : NULL;
}

bool
cluster_lms_r4_drain_request(ClusterLmsSharedState *state, uint64 generation,
							 uint64 *worker_incarnation)
{
	test_drain_request_calls++;
	test_drain_request_generation = generation;
	if (!test_drain_request_succeeds || state != &test_lms_state
		|| worker_incarnation == NULL)
		return false;
	*worker_incarnation = test_drain_worker_incarnation;
	return true;
}

void
cluster_lms_wakeup(int worker_id)
{
	test_lms_wakeup_calls++;
	test_lms_wakeup_worker = worker_id;
}

bool
cluster_cr_server_r4_lmon_reclaim_closed(uint64 worker_incarnation,
										 uint64 generation)
{
	test_reclaim_calls++;
	test_reclaim_worker_incarnation = worker_incarnation;
	test_reclaim_generation = generation;
	return test_reclaim_succeeds;
}

uint64
cluster_gcs_block_dedup_r4_route_purge_closed(void)
{
	test_route_purge_calls++;
	return test_route_purge_result;
}

uint64
cluster_gcs_block_dedup_r4_route_count(void)
{
	test_route_count_calls++;
	return test_route_count;
}

uint64
cluster_gcs_block_r4_requester_count(void)
{
	test_requester_count_calls++;
	return test_requester_count;
}

ClusterUndoSmgrRootMirrorState
cluster_undo_smgr_root_descriptor_read_candidate(
	const char *root_directory,
	uint8 observed[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	test_pgrd_candidate_read_calls++;
	strlcpy(test_pgrd_candidate_root_directory,
			root_directory != NULL ? root_directory : "",
			sizeof(test_pgrd_candidate_root_directory));
	if (test_pgrd_candidate_state
		== CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT && observed != NULL)
		memcpy(observed, test_pgrd_candidate, sizeof(test_pgrd_candidate));
	return test_pgrd_candidate_state;
}

ClusterUndoSmgrRootMirrorState
cluster_undo_smgr_root_descriptor_publish(
	const char *root_directory,
	const uint8 image[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	test_pgrd_publish_calls++;
	strlcpy(test_pgrd_publish_root_directory,
			root_directory != NULL ? root_directory : "",
			sizeof(test_pgrd_publish_root_directory));
	if (image != NULL) {
		memcpy(test_pgrd_published, image, sizeof(test_pgrd_published));
		if (test_pgrd_publish_result
			== CLUSTER_UNDO_SMGR_ROOT_MIRROR_PUBLISHED
			|| test_pgrd_publish_result
				   == CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT) {
			memcpy(test_pgrd_candidate, image, sizeof(test_pgrd_candidate));
			test_pgrd_candidate_state
				= CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
		}
	}
	return test_pgrd_publish_result;
}

bool
cluster_undo_block0_current_startup_fenced_owned(void)
{
	return false;
}

static void
test_read_barrier(void)
{
	pg_read_barrier_impl();
	test_read_barrier_count++;
	if (test_advance_epoch_on_read_barrier == test_read_barrier_count)
		test_current_epoch++;
}

#undef pg_read_barrier
#define pg_read_barrier() test_read_barrier()

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
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

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* Exercise the real product-local policy helpers without exporting a test API. */
#define cluster_undo_block0_r4_prerequisite_snapshot test_r4a_prerequisite_snapshot
#define pg_usleep test_pg_usleep
#ifndef CLUSTER_ACTIVATION_TEST_SOURCE
#define CLUSTER_ACTIVATION_TEST_SOURCE "../../backend/cluster/cluster_semantic_activation.c"
#endif
#include CLUSTER_ACTIVATION_TEST_SOURCE
#undef pg_usleep

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

static pg_atomic_uint64 *
test_gate_u64(Size offset)
{
	return (pg_atomic_uint64 *)(test_semantic_shmem.bytes + offset);
}

static pg_atomic_uint32 *
test_gate_u32(Size offset)
{
	return (pg_atomic_uint32 *)(test_semantic_shmem.bytes + offset);
}

static pg_atomic_uint32 *
test_gate_inflight(ClusterSemanticAdmissionSide side, int feature_index)
{
	return test_gate_u32(TEST_GATE_INFLIGHT_OFFSET
						 + ((Size)side * 64 + (Size)feature_index) * sizeof(pg_atomic_uint32));
}

static void
test_gate_reset(void)
{
	memset(&test_semantic_shmem, 0, sizeof(test_semantic_shmem));
	memset(&test_semantic_utility_mailbox, 0,
		   sizeof(test_semantic_utility_mailbox));
	memset(&test_semantic_ack_table, 0xa5, sizeof(test_semantic_ack_table));
	memset(&test_semantic_pgrd_snapshot, 0xa5,
		   sizeof(test_semantic_pgrd_snapshot));
	memset(&test_semantic_bit22_latch, 0, sizeof(test_semantic_bit22_latch));
	memset(&test_semantic_bit22_seam, 0, sizeof(test_semantic_bit22_seam));
	test_shmem_found = false;
	test_utility_mailbox_found = false;
	test_ack_table_found = false;
	test_pgrd_snapshot_found = false;
	test_bit22_latch_found = false;
	test_bit22_seam_found = false;
	test_shmem_requested_size = 0;
	test_utility_mailbox_requested_size = 0;
	test_ack_table_requested_size = 0;
	test_pgrd_snapshot_requested_size = 0;
	test_bit22_latch_requested_size = 0;
	test_bit22_seam_requested_size = 0;
	test_exit_callback = NULL;
	test_exit_callback_arg = (Datum)0;
	test_exit_registration_count = 0;
	test_current_epoch = 7;
	test_read_barrier_count = 0;
	test_advance_epoch_on_read_barrier = 0;
	ut_r4fsm_census_ok = true;
	ut_activate_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ut_activate_calls = 0;
	ut_create_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ut_create_calls = 0;
	test_r4fsm_root_validate_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	test_r4fsm_bootstrap_read_result = CLUSTER_SEMANTIC_ACTIVATION_OK;
	test_r4fsm_bootstrap_read_ok = false;
	test_r4fsm_bootstrap_implicit_open = false;
	test_restart_unclean = false;
	test_restart_in_recovery = false;
	memset(test_r4fsm_bootstrap_bytes, 0,
		   sizeof(test_r4fsm_bootstrap_bytes));
	test_peer_capability_matches = false;
	test_peer_capability_match_calls = 0;
	test_peer_capability_match_peer = -1;
	test_peer_capability_match_caps = 0;
	test_peer_capability_match_generation = UINT32_MAX;
	test_peer_capability_word_sample_ok = false;
	test_peer_capability_word = 0;
	test_peer_capability_generation = 0;
	memset(test_peer_capability_sample_calls, 0,
		   sizeof(test_peer_capability_sample_calls));
	test_local_capability_word = 0;
	test_ctrc_shmem_is_ready = true;
	memset(test_send_results, 0, sizeof(test_send_results));
	memset(test_send_calls, 0, sizeof(test_send_calls));
	memset(test_send_msg_types, 0, sizeof(test_send_msg_types));
	memset(test_send_payload_lengths, 0,
		   sizeof(test_send_payload_lengths));
	memset(test_send_payloads, 0, sizeof(test_send_payloads));
	memset(test_send_history_msg_types, 0,
		   sizeof(test_send_history_msg_types));
	memset(test_send_history_payload_lengths, 0,
		   sizeof(test_send_history_payload_lengths));
	memset(test_send_history_payloads, 0,
		   sizeof(test_send_history_payloads));
	memset(test_close_calls, 0, sizeof(test_close_calls));
	test_phase3_observe_calls = 0;
	memset(&test_phase3_observed_item, 0, sizeof(test_phase3_observed_item));
	test_admitted_snapshot_calls = 0;
	test_admitted_snapshot_valid = false;
	memset(&test_admitted_snapshot_episode, 0,
		   sizeof(test_admitted_snapshot_episode));
	memset(&test_admitted_snapshot_marker, 0,
		   sizeof(test_admitted_snapshot_marker));
	test_initial_clean_snapshot_calls = 0;
	test_initial_clean_snapshot_valid = false;
	memset(&test_initial_clean_snapshot, 0,
		   sizeof(test_initial_clean_snapshot));
	test_grd_recovery_state = GRD_RECOVERY_IDLE;
	test_membership_snapshot_calls = 0;
	test_membership_snapshot_fail_at_call = 0;
	test_membership_snapshot_valid = true;
	test_membership_snapshot_lo = UINT64_C(0x0f);
	test_membership_snapshot_hi = 0;
	test_membership_snapshot_epoch = test_current_epoch;
	test_r4a_snapshot_calls = 0;
	memset(&test_r4a_snapshot, 0, sizeof(test_r4a_snapshot));
	test_r4a_snapshot.target_node_id = -1;
	test_wait_sleep_calls = 0;
	test_complete_after_wait_sleeps = 0;
	test_wait_completion_request_seq = 0;
	test_wait_completion_succeeded = false;
	memset(&test_lms_state, 0, sizeof(test_lms_state));
	test_lms_state_available = true;
	test_drain_request_succeeds = true;
	test_drain_worker_incarnation = UINT64_C(8);
	test_drain_request_calls = 0;
	test_drain_request_generation = 0;
	test_lms_wakeup_calls = 0;
	test_lms_wakeup_worker = -1;
	test_reclaim_succeeds = false;
	test_reclaim_calls = 0;
	test_reclaim_worker_incarnation = 0;
	test_reclaim_generation = 0;
	test_route_purge_result = 0;
	test_route_purge_calls = 0;
	test_route_count = 0;
	test_route_count_calls = 0;
	test_requester_count = 0;
	test_requester_count_calls = 0;
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_ABSENT;
	memset(test_pgrd_candidate, 0, sizeof(test_pgrd_candidate));
	test_pgrd_candidate_read_calls = 0;
	test_pgrd_candidate_root_directory[0] = '\0';
	test_pgrd_publish_result = CLUSTER_UNDO_SMGR_ROOT_MIRROR_PUBLISHED;
	test_pgrd_publish_calls = 0;
	test_pgrd_publish_root_directory[0] = '\0';
	memset(test_pgrd_published, 0, sizeof(test_pgrd_published));
	test_strong_random_calls = 0;
	test_system_identifier = 0;
	test_qvotec_in_quorum = true;
	test_qvotec_self_incarnation = UINT64_C(0x445566778899aabb);
	test_last_admitted_incarnation = UINT64_C(0x445566778899aabb);
	memset(test_remote_admitted_incarnations, 0,
		   sizeof(test_remote_admitted_incarnations));
	memset(test_observed_slot_valid, 0, sizeof(test_observed_slot_valid));
	memset(test_observed_slot_incarnation, 0,
		   sizeof(test_observed_slot_incarnation));
	memset(test_observed_slot_generation, 0,
		   sizeof(test_observed_slot_generation));
	memset(test_observed_slot_epoch, 0,
		   sizeof(test_observed_slot_epoch));
	test_resource_x_gate_snapshot_valid = false;
	memset(&test_resource_x_gate_snapshot, 0,
		   sizeof(test_resource_x_gate_snapshot));
	test_resource_x_cutover_digest_valid = false;
	memset(&test_resource_x_cutover_token, 0,
		   sizeof(test_resource_x_cutover_token));
	test_resource_x_cutover_thawed = false;
	test_resource_x_cutover_digest = 0;
	cluster_shared_data_dir = NULL;
	MyProcPid = 101;
	cluster_node_id = 1;
	SemanticActivationShmem = NULL;
	SemanticActivationUtilityMailbox = NULL;
	SemanticActivationAckTable = NULL;
	SemanticActivationPgrdSnapshot = NULL;
	SemanticActivationBit22Latch = NULL;
	SemanticActivationBit22Seam = NULL;
	memset(semantic_activation_local_inflight, 0, sizeof(semantic_activation_local_inflight));
	semantic_activation_exit_hook_pid = 0;
	semantic_activation_lmon_record_read_seq = 0;
	semantic_activation_lmon_pgrd_request_seq = 0;
	semantic_activation_lmon_pgrd_utility_request_seq = 0;
	semantic_activation_lmon_pgrd_candidate_request_seq = 0;
	memset(semantic_activation_lmon_pgrd_candidate, 0,
		   sizeof(semantic_activation_lmon_pgrd_candidate));
	memset(&semantic_activation_lmon_pgrd_formation, 0,
		   sizeof(semantic_activation_lmon_pgrd_formation));
	semantic_activation_lmon_pgrd_read_request_seq = 0;
	semantic_activation_lmon_pgrd_read_utility_request_seq = 0;
	memset(&semantic_activation_lmon_pgrd_read_formation, 0,
		   sizeof(semantic_activation_lmon_pgrd_read_formation));
	semantic_activation_lmon_member_pgrd_read_request_seq = 0;
	semantic_activation_lmon_prepare_cas_seq = 0;
	semantic_activation_lmon_prepare_cas_utility_request_seq = 0;
	semantic_activation_lmon_commit_cas_seq = 0;
	semantic_activation_lmon_commit_cas_utility_request_seq = 0;
	semantic_activation_lmon_open_cas_seq = 0;
	semantic_activation_lmon_open_cas_utility_request_seq = 0;
	/* RF-ROOT P9 verification part 3 (cold-formation): the bit22 round's PREPARE
	 * CAS driver state is per-round — reset it like the R4 CAS state. */
	semantic_activation_lmon_bit22_prepare_cas_seq = 0;
	semantic_activation_lmon_bit22_prepare_cas_done = false;
	semantic_activation_ack_ingress_init(
		&semantic_activation_ack_local_ingress);
	memset(&semantic_activation_ack_local_pending_send, 0,
		   sizeof(semantic_activation_ack_local_pending_send));
	memset(&semantic_activation_ack_local_request_origin, 0,
		   sizeof(semantic_activation_ack_local_request_origin));
	cluster_semantic_activation_shmem_init();
}

static void
test_gate_publish(uint64 seq, uint64 active_bits, uint64 generation, uint64 formation_epoch,
				  bool closed)
{
	pg_atomic_write_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET), seq);
	pg_atomic_write_u64(test_gate_u64(TEST_GATE_ACTIVE_BITS_OFFSET), active_bits);
	pg_atomic_write_u64(test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET), generation);
	pg_atomic_write_u64(test_gate_u64(TEST_GATE_FORMATION_EPOCH_OFFSET), formation_epoch);
	pg_atomic_write_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET), closed ? 1 : 0);
}

static uint64
test_token_formation_epoch(const ClusterSemanticAdmissionToken *token)
{
	uint64 formation_epoch;

	memcpy(&formation_epoch, ((const uint8 *)token) + 16, sizeof(formation_epoch));
	return formation_epoch;
}

static SemanticActivationAckTuple
valid_ack(void)
{
	return (SemanticActivationAckTuple){
		.node_id = 7,
		.boot_id = UINT64_C(0x101),
		.admitted_incarnation = UINT64_C(0x202),
		.control_connection_generation = UINT64_C(0x303),
		.capability_word = UINT32_C(0x00303000),
		.capability_generation = UINT64_C(0x404),
		.transition_epoch = UINT64_C(0x505),
		.record_generation = UINT64_C(0x606),
	};
}

#define DEFINE_STATE_VALUE_TEST(test_name, state_value, expected_value)                            \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		UT_ASSERT_EQ((state_value), (expected_value));                                             \
	}

#define DEFINE_FSM_EDGE_TEST(test_name, reverse_value, from_value, to_value)                       \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		SemanticActivationState next = SEMANTIC_ACTIVATION_STATE_INVALID;                          \
		UT_ASSERT(semantic_activation_fsm_next((from_value), (reverse_value), &next));             \
		UT_ASSERT_EQ(next, (to_value));                                                            \
	}

#define DEFINE_CALLBACK_TEST(test_name, state_value, callback_value)                               \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		UT_ASSERT_EQ(semantic_activation_callback_for_state((state_value)), (callback_value));     \
	}

#define DEFINE_INVALID_SELF_EDGE_TEST(test_name, state_value)                                      \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		SemanticActivationState next = (state_value);                                              \
		UT_ASSERT(!semantic_activation_fsm_next((state_value), false, &next));                     \
		UT_ASSERT_EQ(next, (state_value));                                                         \
	}

#define DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_name, state_value)                                  \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		SemanticActivationState next = (state_value);                                              \
		UT_ASSERT(semantic_activation_fsm_next((state_value), false, &next));                      \
		UT_ASSERT(next != (state_value));                                                          \
	}

#define DEFINE_FAILURE_TEST(test_name, state_value, expected_closed, expected_revert)              \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		SemanticActivationFailurePolicy policy;                                                    \
		memset(&policy, 0, sizeof(policy));                                                        \
		UT_ASSERT(semantic_activation_failure_policy((state_value), &policy));                     \
		UT_ASSERT_EQ(policy.target, SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN);                        \
		UT_ASSERT_EQ(policy.admission_closed_until_source_open, (expected_closed));                \
		UT_ASSERT_EQ(policy.revert_source_closed, (expected_revert));                              \
	}

UT_TEST(test_01_feature_bit_is_one)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, UINT64_C(1));
}

UT_TEST(test_02_required_hello_caps_are_frozen)
{
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1, UINT32_C(0x00001000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1, UINT32_C(0x00002000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1, UINT32_C(0x00100000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1, UINT32_C(0x00200000));
}

UT_TEST(test_03_action_values_are_frozen)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ENABLE_ALL, 0);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_DISABLE_ALL, 1);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ROLLBACK_ALL, 2);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ROLLBACK_ABORT, 3);
}

UT_TEST(test_04_admission_values_are_frozen)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ADMISSION_OK, 0);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT, 1);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED, 2);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED, 3);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ADMISSION_CLOSED, 4);
}

UT_TEST(test_05_activation_result_values_are_frozen)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_OK, 0);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED, 3);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD, 9);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT, 10);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 12);
}

UT_TEST(test_06_admission_side_values_are_frozen)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_SOURCE_SIDE, 0);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_TARGET_SIDE, 1);
}

UT_TEST(test_07_r4_descriptor_identity)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();

	UT_ASSERT_NOT_NULL(descriptor);
	UT_ASSERT_STR_EQ(descriptor->name, "R4_SYNC_CR_V1");
	UT_ASSERT_EQ(descriptor->feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
}

UT_TEST(test_08_r4_descriptor_caps_and_active_bits)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();

	UT_ASSERT_EQ(descriptor->required_hello_caps, UINT32_C(0x00303000));
	UT_ASSERT_EQ(descriptor->required_active_bits, 0);
}

UT_TEST(test_09_r4_descriptor_retains_source)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();

	UT_ASSERT(descriptor->source_available);
}

UT_TEST(test_10_r4_descriptor_has_every_callback)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();

	UT_ASSERT_NOT_NULL(descriptor->pre_prepare_readiness);
	UT_ASSERT_NOT_NULL(descriptor->close_source_admission);
	UT_ASSERT_NOT_NULL(descriptor->source_logical_debt_zero);
	UT_ASSERT_NOT_NULL(descriptor->source_transport_zero);
	UT_ASSERT_NOT_NULL(descriptor->prepare_target);
	UT_ASSERT_NOT_NULL(descriptor->apply_target_closed);
	UT_ASSERT_NOT_NULL(descriptor->revert_source_closed);
	UT_ASSERT_NOT_NULL(descriptor->open_target_admission);
}

UT_TEST(test_10a_r11_resource_x_cutover_descriptor_is_compiled_exact)
{
	const ClusterSemanticActivationDescriptor *r4
		= cluster_semantic_activation_r4_descriptor();
	const ClusterSemanticActivationDescriptor *cutover
		= cluster_semantic_activation_r11_resource_x_descriptor();
	ClusterSemanticActivationRefusal refusal;
	ClusterSemanticZeroProof proof;

	UT_ASSERT_NOT_NULL(cutover);
	UT_ASSERT_STR_EQ(cutover->name, "R11_RESOURCE_X_D5_CUTOVER_V1");
	UT_ASSERT_EQ(cutover->feature_bit,
				 CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1);
	UT_ASSERT_EQ(cutover->required_hello_caps,
				 PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
				 | PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1);
	UT_ASSERT_EQ(cutover->required_active_bits, 0);
	UT_ASSERT(!cutover->source_available);
	UT_ASSERT_NOT_NULL(cutover->pre_prepare_readiness);
	UT_ASSERT_NOT_NULL(cutover->close_source_admission);
	UT_ASSERT_NOT_NULL(cutover->source_logical_debt_zero);
	UT_ASSERT_NOT_NULL(cutover->source_transport_zero);
	UT_ASSERT_NOT_NULL(cutover->prepare_target);
	UT_ASSERT_NOT_NULL(cutover->apply_target_closed);
	UT_ASSERT_NOT_NULL(cutover->revert_source_closed);
	UT_ASSERT_NOT_NULL(cutover->open_target_admission);
	UT_ASSERT_EQ(cluster_semantic_activation_descriptor(
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1), r4);
	UT_ASSERT_EQ(cluster_semantic_activation_descriptor(
				 CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1),
				 cutover);
	UT_ASSERT_EQ(cluster_semantic_activation_compiled_feature_bitmap(),
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
				 | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1);
	/* Source removal does not remove the native Resource-X owner.  R4 only
	 * revalidates freshness: a nonzero native Resource-X formation is a
	 * separate numeric domain and need not equal the R4 generation. */
	test_gate_reset();
	test_gate_publish(2, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		7, test_current_epoch, false);
	test_resource_x_gate_snapshot_valid = true;
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_OPEN;
	test_resource_x_gate_snapshot.formation = 17;
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(cutover->pre_prepare_readiness(7, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(refusal.feature_bit, UINT64_C(0));
	UT_ASSERT_EQ(refusal.expected_generation, 7);
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_FROZEN;
	UT_ASSERT_EQ(cutover->pre_prepare_readiness(7, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_OPEN;
	UT_ASSERT_EQ(cutover->pre_prepare_readiness(8, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);

	/* No exact SOURCE_CLOSED ACK image or same-T proof exists yet. */
	memset(&proof, 0x7f, sizeof(proof));
	UT_ASSERT_EQ(cutover->source_logical_debt_zero(7, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(proof.record_generation, 0);
	UT_ASSERT_EQ(proof.debt_count, 0);
	UT_ASSERT_EQ(proof.sample_digest, 0);
	memset(&proof, 0x7f, sizeof(proof));
	UT_ASSERT_EQ(cutover->source_transport_zero(7, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(proof.record_generation, 0);
	UT_ASSERT_EQ(proof.debt_count, 0);
	UT_ASSERT_EQ(proof.sample_digest, 0);
	UT_ASSERT_EQ(cutover->close_source_admission(7),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(cutover->prepare_target(7),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(cutover->apply_target_closed(7),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(cutover->revert_source_closed(7),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(cutover->open_target_admission(7),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	test_gate_reset();
}

UT_TEST(test_10b_r11_writer_selector_snapshots_one_exact_gate_generation)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r11_resource_x_descriptor();
	ClusterSemanticR11CutoverSnapshot cutover;
	ClusterSemanticActivationAckTableV1 *table;
	ClusterSemanticZeroProof proof;
	uint32 resource_x_caps
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
		  | PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	uint64 generation = UINT64_MAX;
	uint64 sample_digest = 0;
	int node;

	test_gate_reset();
	test_gate_publish(2, 0, 19, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_resource_x_writer_path_snapshot(&generation),
				 RESOURCE_X_WRITER_CLOSED);
	UT_ASSERT_EQ(generation, UINT64_C(19));

	test_gate_publish(4,
				  CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
				  20, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_resource_x_writer_path_snapshot(&generation),
				 RESOURCE_X_WRITER_TARGET);
	UT_ASSERT_EQ(generation, UINT64_C(20));

	test_gate_publish(6,
				  CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
				  21, test_current_epoch, true);
	UT_ASSERT_EQ(cluster_resource_x_writer_path_snapshot(&generation),
				 RESOURCE_X_WRITER_CLOSED);
	UT_ASSERT_EQ(generation, UINT64_C(21));

	/* A torn gate and a missing output owner both fail closed. */
	test_gate_publish(7, 0, 22, test_current_epoch, false);
	generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_resource_x_writer_path_snapshot(&generation),
				 RESOURCE_X_WRITER_CLOSED);
	UT_ASSERT_EQ(generation, UINT64_C(0));
	UT_ASSERT_EQ(cluster_resource_x_writer_path_snapshot(NULL),
				 RESOURCE_X_WRITER_CLOSED);

	/* Exact R11 SOURCE_CLOSED is a read-only co-sample of the admission gate,
	 * current formation/capabilities, and the complete SAMPLE ACK image.  The
	 * initial homogeneous formation deliberately keeps membership epoch zero;
	 * Resource-X must derive its nonzero predecessor from the exact R4 round,
	 * never manufacture membership epoch one. */
	test_gate_reset();
	test_current_epoch = 0;
	test_membership_snapshot_epoch = 0;
	test_local_capability_word = resource_x_caps;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_matches = true;
	test_peer_capability_word = resource_x_caps;
	test_peer_capability_generation = 19;
	for (node = 0; node < 4; node++)
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;
	for (node = 0; node < 4; node++) {
		test_observed_slot_valid[node] = true;
		test_observed_slot_incarnation[node]
			= node == cluster_node_id
				  ? test_qvotec_self_incarnation
				  : test_remote_admitted_incarnations[node];
		test_observed_slot_generation[node] = UINT64_C(41) + (uint64)node;
		test_observed_slot_epoch[node] = 7;
	}
	table = SemanticActivationAckTable;
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->coordinator_node = 0;
	table->round_nonce = 41;
	table->expected_members_lo = UINT64_C(0x0f);
	table->observed_members_lo = UINT64_C(0x0f);
	table->transition_epoch = test_current_epoch;
	table->record_generation = 23;
	table->source_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	table->target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	table->capability_sample_digest = 0;
	for (node = 0; node < 4; node++) {
		if (node == cluster_node_id) {
			UT_ASSERT(semantic_activation_ack_self_tuple(
				node, resource_x_caps, test_current_epoch, 23,
				&table->expected[node]));
		} else {
			table->expected[node].node_id = (uint32)node;
			table->expected[node].boot_id
				= test_remote_admitted_incarnations[node];
			table->expected[node].admitted_incarnation
				= test_remote_admitted_incarnations[node];
			table->expected[node].control_connection_generation = 19;
			table->expected[node].capability_word = resource_x_caps;
			table->expected[node].capability_generation = 19;
			table->expected[node].transition_epoch = test_current_epoch;
			table->expected[node].record_generation = 23;
		}
		table->observed[node] = table->expected[node];
	}
	UT_ASSERT(semantic_activation_ack_sample_digest(
		table, &sample_digest));
	UT_ASSERT(sample_digest != 0);
	test_gate_publish(8,
				  CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
				  23, test_current_epoch, true);
	UT_ASSERT(cluster_semantic_activation_r11_cutover_snapshot(&cutover));
	UT_ASSERT_EQ(cutover.phase,
				 CLUSTER_SEMANTIC_R11_CUTOVER_SOURCE_CLOSED);
	UT_ASSERT_EQ(cutover.record_generation, UINT64_C(23));
	UT_ASSERT_EQ(cutover.formation_epoch, test_current_epoch);
	test_resource_x_cutover_digest_valid = true;
	/* Deliberately unrelated domains: R4 generation 23, Resource-X 17->18. */
	test_resource_x_cutover_token.old_formation = 17;
	test_resource_x_cutover_token.new_formation = 18;
	test_resource_x_cutover_token.freeze_generation = 1;
	test_resource_x_cutover_thawed = false;
	test_resource_x_cutover_digest = UINT64_C(0xa55a9911);
	test_resource_x_gate_snapshot_valid = true;
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_FROZEN;
	test_resource_x_gate_snapshot.formation = 17;
	test_resource_x_gate_snapshot.freeze_generation = 1;
	UT_ASSERT_EQ(descriptor->close_source_admission(23),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	memset(&proof, 0, sizeof(proof));
	UT_ASSERT_EQ(descriptor->source_logical_debt_zero(23, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(proof.record_generation, UINT64_C(23));
	UT_ASSERT_EQ(proof.debt_count, UINT64_C(0));
	UT_ASSERT_EQ(proof.sample_digest, UINT64_C(0xa55a9911));
	memset(&proof, 0, sizeof(proof));
	UT_ASSERT_EQ(descriptor->source_transport_zero(23, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(proof.sample_digest, UINT64_C(0xa55a9911));
	UT_ASSERT_EQ(descriptor->prepare_target(23),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(descriptor->apply_target_closed(23),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(descriptor->revert_source_closed(23),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(descriptor->open_target_admission(23),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);

	/* Durable OPEN is one record generation ahead of the still-closed local
	 * selector.  It becomes TARGET_OPEN only after local publication. */
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
	table->capability_sample_digest = sample_digest;
	table->record_generation = 25;
	for (node = 0; node < 4; node++) {
		table->expected[node].record_generation = 25;
		table->observed[node].record_generation = 25;
	}
	test_gate_publish(10,
				  CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
				  24, test_current_epoch, true);
	UT_ASSERT(cluster_semantic_activation_r11_cutover_snapshot(&cutover));
	UT_ASSERT_EQ(cutover.phase,
				 CLUSTER_SEMANTIC_R11_CUTOVER_DURABLE_OPEN_PENDING_LOCAL);
	UT_ASSERT_EQ(cutover.record_generation, UINT64_C(23));
	test_resource_x_cutover_thawed = true;
	UT_ASSERT_EQ(descriptor->open_target_admission(23),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(descriptor->open_target_admission(24),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	test_gate_publish(12, table->target_feature_bitmap, 25,
				  test_current_epoch, false);
	UT_ASSERT(cluster_semantic_activation_r11_cutover_snapshot(&cutover));
	UT_ASSERT_EQ(cutover.phase,
				 CLUSTER_SEMANTIC_R11_CUTOVER_TARGET_OPEN);

	/* Any round or generation drift is not a cutover phase. */
	test_gate_publish(12,
				  CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
				  22, test_current_epoch, true);
	UT_ASSERT(!cluster_semantic_activation_r11_cutover_snapshot(&cutover));
	UT_ASSERT_EQ(cutover.phase, CLUSTER_SEMANTIC_R11_CUTOVER_NONE);
	test_gate_reset();
}

UT_TEST(test_10c_r4_carrier_selects_one_exact_compiled_feature_round)
{
	const ClusterSemanticActivationDescriptor *descriptor = NULL;
	uint32 required_caps = 0;
	uint64 r4 = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	uint64 r11 = CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;

	UT_ASSERT(semantic_activation_round_descriptor(
		0, r4, 0, &descriptor, &required_caps));
	UT_ASSERT_EQ(descriptor, cluster_semantic_activation_r4_descriptor());
	UT_ASSERT_EQ(required_caps,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
				 | descriptor->required_hello_caps);

	descriptor = NULL;
	required_caps = 0;
	UT_ASSERT(semantic_activation_round_descriptor(
		r4, r4 | r11, 0, &descriptor, &required_caps));
	UT_ASSERT_EQ(descriptor,
				 cluster_semantic_activation_r11_resource_x_descriptor());
	UT_ASSERT_EQ(required_caps,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
				 | descriptor->required_hello_caps);

	/* R4 remains the carrier for every later exact one-feature round. */
	UT_ASSERT(!semantic_activation_round_descriptor(
		0, r11, 0, &descriptor, &required_caps));
	UT_ASSERT(!semantic_activation_round_descriptor(
		0, r4 | r11, 0, &descriptor, &required_caps));
	UT_ASSERT(!semantic_activation_round_descriptor(
		r4 | r11, r4, 0, &descriptor, &required_caps));
	UT_ASSERT(!semantic_activation_round_descriptor(
		r4, r4, 0, &descriptor, &required_caps));
	UT_ASSERT(!semantic_activation_round_descriptor(
		r4, r4 | (UINT64_C(1) << 9), 0,
		&descriptor, &required_caps));
	UT_ASSERT(!semantic_activation_round_descriptor(
		r4, r4 | r11, r11, &descriptor, &required_caps));
	UT_ASSERT(!semantic_activation_round_descriptor(
		r4, r4 | r11, 0, NULL, &required_caps));
	UT_ASSERT(!semantic_activation_round_descriptor(
		r4, r4 | r11, 0, &descriptor, NULL));
}

UT_TEST(test_11_source_only_is_exclusive)
{
	UT_ASSERT(semantic_activation_source_target_exclusive(true, false));
}

UT_TEST(test_12_target_only_is_exclusive)
{
	UT_ASSERT(semantic_activation_source_target_exclusive(false, true));
	UT_ASSERT(semantic_activation_source_target_exclusive(false, false));
	UT_ASSERT(!semantic_activation_source_target_exclusive(true, true));
}

DEFINE_FSM_EDGE_TEST(test_13_enable_source_open_to_admission_stopped, false,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED)
DEFINE_FSM_EDGE_TEST(test_14_enable_admission_stopped_to_drain, false,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY)
DEFINE_FSM_EDGE_TEST(test_15_enable_drain_to_logical_zero, false,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO)
DEFINE_FSM_EDGE_TEST(test_16_enable_logical_zero_to_transport_barrier, false,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO,
					 SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER)
DEFINE_FSM_EDGE_TEST(test_17_enable_transport_barrier_to_transport_zero, false,
					 SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER,
					 SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO)
DEFINE_FSM_EDGE_TEST(test_18_enable_transport_zero_to_epoch_barrier, false,
					 SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO,
					 SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER)
DEFINE_FSM_EDGE_TEST(test_19_enable_epoch_barrier_to_target_staged, false,
					 SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER,
					 SEMANTIC_ACTIVATION_STATE_TARGET_STAGED)
DEFINE_FSM_EDGE_TEST(test_20_enable_target_staged_to_committed_closed, false,
					 SEMANTIC_ACTIVATION_STATE_TARGET_STAGED,
					 SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED)
DEFINE_FSM_EDGE_TEST(test_21_enable_committed_closed_to_target_open, false,
					 SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED,
					 SEMANTIC_ACTIVATION_STATE_TARGET_OPEN)

UT_TEST(test_22_enable_target_open_is_terminal)
{
	SemanticActivationState next = SEMANTIC_ACTIVATION_STATE_INVALID;

	UT_ASSERT(!semantic_activation_fsm_next(SEMANTIC_ACTIVATION_STATE_TARGET_OPEN, false, &next));
}

DEFINE_FSM_EDGE_TEST(test_23_disable_source_open_to_admission_stopped, true,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED)
DEFINE_FSM_EDGE_TEST(test_24_disable_admission_stopped_to_drain, true,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY)
DEFINE_FSM_EDGE_TEST(test_25_disable_drain_to_logical_zero, true,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO)
DEFINE_FSM_EDGE_TEST(test_26_disable_logical_zero_to_transport_barrier, true,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO,
					 SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER)
DEFINE_FSM_EDGE_TEST(test_27_disable_transport_barrier_to_transport_zero, true,
					 SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER,
					 SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO)
DEFINE_FSM_EDGE_TEST(test_28_disable_transport_zero_to_epoch_barrier, true,
					 SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO,
					 SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER)
DEFINE_FSM_EDGE_TEST(test_29_disable_epoch_barrier_to_target_staged, true,
					 SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER,
					 SEMANTIC_ACTIVATION_STATE_TARGET_STAGED)
DEFINE_FSM_EDGE_TEST(test_30_disable_target_staged_to_committed_closed, true,
					 SEMANTIC_ACTIVATION_STATE_TARGET_STAGED,
					 SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED)
DEFINE_FSM_EDGE_TEST(test_31_disable_committed_closed_to_target_open, true,
					 SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED,
					 SEMANTIC_ACTIVATION_STATE_TARGET_OPEN)

UT_TEST(test_32_disable_target_open_is_terminal)
{
	SemanticActivationState next = SEMANTIC_ACTIVATION_STATE_INVALID;

	UT_ASSERT(!semantic_activation_fsm_next(SEMANTIC_ACTIVATION_STATE_TARGET_OPEN, true, &next));
}

DEFINE_CALLBACK_TEST(test_33_admission_stop_calls_close_source,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED,
					 SEMANTIC_ACTIVATION_CALLBACK_CLOSE_SOURCE)
DEFINE_CALLBACK_TEST(test_34_drain_has_no_eraser_callback,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY,
					 SEMANTIC_ACTIVATION_CALLBACK_NONE)
DEFINE_CALLBACK_TEST(test_35_logical_zero_calls_logical_proof,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO,
					 SEMANTIC_ACTIVATION_CALLBACK_LOGICAL_ZERO)
DEFINE_CALLBACK_TEST(test_36_ordered_barrier_calls_transport_barrier,
					 SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER,
					 SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_BARRIER)
DEFINE_CALLBACK_TEST(test_37_transport_zero_calls_transport_proof,
					 SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO,
					 SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_ZERO)
DEFINE_CALLBACK_TEST(test_38_epoch_state_calls_exact_ack_barrier,
					 SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER,
					 SEMANTIC_ACTIVATION_CALLBACK_EPOCH_CAPABILITY_BARRIER)
DEFINE_CALLBACK_TEST(test_39_target_staged_calls_prepare, SEMANTIC_ACTIVATION_STATE_TARGET_STAGED,
					 SEMANTIC_ACTIVATION_CALLBACK_PREPARE_TARGET)
DEFINE_CALLBACK_TEST(test_40_committed_closed_calls_apply,
					 SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED,
					 SEMANTIC_ACTIVATION_CALLBACK_APPLY_TARGET_CLOSED)
DEFINE_CALLBACK_TEST(test_41_target_open_calls_open_admission,
					 SEMANTIC_ACTIVATION_STATE_TARGET_OPEN,
					 SEMANTIC_ACTIVATION_CALLBACK_OPEN_TARGET)
DEFINE_CALLBACK_TEST(test_42_source_open_has_no_transition_callback,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN, SEMANTIC_ACTIVATION_CALLBACK_NONE)

DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_43_source_open_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_44_admission_stopped_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_45_drain_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_46_logical_zero_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_47_transport_barrier_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_48_transport_zero_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_49_epoch_barrier_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_50_target_staged_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_TARGET_STAGED)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_51_committed_closed_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED)
DEFINE_INVALID_SELF_EDGE_TEST(test_52_target_open_has_no_self_edge,
							  SEMANTIC_ACTIVATION_STATE_TARGET_OPEN)

UT_TEST(test_53_invalid_low_state_has_no_edge)
{
	SemanticActivationState next = SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN;

	UT_ASSERT(!semantic_activation_fsm_next(SEMANTIC_ACTIVATION_STATE_INVALID, false, &next));
}

UT_TEST(test_54_invalid_high_state_has_no_edge)
{
	SemanticActivationState next = SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN;

	UT_ASSERT(!semantic_activation_fsm_next((SemanticActivationState)10, false, &next));
}

DEFINE_FAILURE_TEST(test_55_failure_at_source_open_restores_source,
					SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN, false, false)
DEFINE_FAILURE_TEST(test_56_failure_after_admission_stop_restores_source,
					SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED, true, false)
DEFINE_FAILURE_TEST(test_57_failure_during_drain_restores_source,
					SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY, true, false)
DEFINE_FAILURE_TEST(test_58_failure_after_logical_zero_restores_source,
					SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO, true, false)
DEFINE_FAILURE_TEST(test_59_failure_during_ordered_barrier_restores_source,
					SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER, true, false)
DEFINE_FAILURE_TEST(test_60_failure_after_transport_zero_restores_source,
					SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO, true, false)
DEFINE_FAILURE_TEST(test_61_failure_at_epoch_barrier_restores_source,
					SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER, true, false)
DEFINE_FAILURE_TEST(test_62_failure_after_prepare_restores_source,
					SEMANTIC_ACTIVATION_STATE_TARGET_STAGED, true, false)
DEFINE_FAILURE_TEST(test_63_failure_after_commit_requires_revert_closed,
					SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED, true, true)

UT_TEST(test_64_target_open_is_not_reinterpreted_as_transition_failure)
{
	SemanticActivationFailurePolicy policy;

	memset(&policy, 0, sizeof(policy));
	UT_ASSERT(!semantic_activation_failure_policy(SEMANTIC_ACTIVATION_STATE_TARGET_OPEN, &policy));
}

UT_TEST(test_65_identical_ack_tuple_matches)
{
	SemanticActivationAckTuple a = valid_ack();
	SemanticActivationAckTuple b = a;

	UT_ASSERT(semantic_activation_ack_matches(&a, &b));
}

#define DEFINE_ACK_INVALIDATION_TEST(test_name, field_name)                                        \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		SemanticActivationAckTuple expected = valid_ack();                                         \
		SemanticActivationAckTuple observed = expected;                                            \
		observed.field_name++;                                                                     \
		UT_ASSERT(!semantic_activation_ack_matches(&observed, &expected));                         \
	}

DEFINE_ACK_INVALIDATION_TEST(test_66_node_change_invalidates_ack, node_id)
DEFINE_ACK_INVALIDATION_TEST(test_67_boot_change_invalidates_ack, boot_id)
DEFINE_ACK_INVALIDATION_TEST(test_68_incarnation_change_invalidates_ack, admitted_incarnation)
DEFINE_ACK_INVALIDATION_TEST(test_69_control_reconnect_invalidates_ack,
							 control_connection_generation)
DEFINE_ACK_INVALIDATION_TEST(test_70_capability_word_change_invalidates_ack, capability_word)
DEFINE_ACK_INVALIDATION_TEST(test_71_capability_generation_change_invalidates_ack,
							 capability_generation)
DEFINE_ACK_INVALIDATION_TEST(test_72_epoch_change_invalidates_ack, transition_epoch)
DEFINE_ACK_INVALIDATION_TEST(test_73_record_generation_change_invalidates_ack, record_generation)

UT_TEST(test_74_null_observed_ack_never_matches)
{
	SemanticActivationAckTuple expected = valid_ack();

	UT_ASSERT(!semantic_activation_ack_matches(NULL, &expected));
}

UT_TEST(test_75_null_expected_ack_never_matches)
{
	SemanticActivationAckTuple observed = valid_ack();

	UT_ASSERT(!semantic_activation_ack_matches(&observed, NULL));
}

UT_TEST(test_75a_d13_full_ack_table_requires_exact_member_set_and_tuples)
{
	SemanticActivationAckTuple expected[CLUSTER_MAX_NODES];
	SemanticActivationAckTuple observed[CLUSTER_MAX_NODES];
	uint64 members_lo = (UINT64_C(1) << 1) | (UINT64_C(1) << 3);

	memset(expected, 0, sizeof(expected));
	memset(observed, 0, sizeof(observed));
	expected[1] = valid_ack();
	expected[1].node_id = 1;
	expected[3] = valid_ack();
	expected[3].node_id = 3;
	expected[3].boot_id++;
	observed[1] = expected[1];
	observed[3] = expected[3];

	UT_ASSERT(semantic_activation_full_ack_table_matches(
		observed, members_lo, 0, expected, members_lo, 0));
	observed[3].capability_generation++;
	UT_ASSERT(!semantic_activation_full_ack_table_matches(
		observed, members_lo, 0, expected, members_lo, 0));
	observed[3] = expected[3];
	UT_ASSERT(!semantic_activation_full_ack_table_matches(
		observed, members_lo & ~(UINT64_C(1) << 3), 0,
		expected, members_lo, 0));
	UT_ASSERT(!semantic_activation_full_ack_table_matches(
		observed, members_lo | (UINT64_C(1) << 4), 0,
		expected, members_lo, 0));
}

UT_TEST(test_76_inactive_feature_source_is_admitted)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, false, CLUSTER_SEMANTIC_SOURCE_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_OK);
}

UT_TEST(test_77_inactive_feature_target_is_disabled)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, false, CLUSTER_SEMANTIC_TARGET_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED);
}

UT_TEST(test_78_active_feature_source_is_dormant)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 1, false, CLUSTER_SEMANTIC_SOURCE_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT);
}

UT_TEST(test_79_active_feature_target_is_admitted)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 1, false, CLUSTER_SEMANTIC_TARGET_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_OK);
}

UT_TEST(test_80_transition_closes_source_admission)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, true, CLUSTER_SEMANTIC_SOURCE_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_CLOSED);
}

UT_TEST(test_81_transition_closes_target_admission)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, true, CLUSTER_SEMANTIC_TARGET_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_CLOSED);
}

UT_TEST(test_82_source_generation_change_is_typed)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, false, CLUSTER_SEMANTIC_SOURCE_SIDE, 4, 5),
		CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED);
}

UT_TEST(test_83_target_generation_change_is_typed)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 1, false, CLUSTER_SEMANTIC_TARGET_SIDE, 4, 5),
		CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED);
}

UT_TEST(test_84_unknown_side_is_closed)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, false, (ClusterSemanticAdmissionSide)2, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_CLOSED);
}

UT_TEST(test_85_zero_feature_is_closed)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(0, 0, false, CLUSTER_SEMANTIC_SOURCE_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_CLOSED);
}

UT_TEST(test_85a_modifier_gate_requires_durable_source_or_target_open)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, true, CLUSTER_SEMANTIC_SOURCE_SIDE, 0, 0),
		CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT(!semantic_activation_modifier_policy(0, 0, true));
	UT_ASSERT(semantic_activation_modifier_policy(0, 8, false));
	UT_ASSERT(semantic_activation_modifier_policy(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 9, false));
	UT_ASSERT(semantic_activation_modifier_policy(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
			| CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		12, false));
}

UT_TEST(test_85b_modifier_gate_closes_replacement_until_uniform_open)
{
	UT_ASSERT(!semantic_activation_modifier_policy(0, 9, true));
	UT_ASSERT(!semantic_activation_modifier_policy(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 9, true));
	UT_ASSERT(!semantic_activation_modifier_policy(
		CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		12, false));
	UT_ASSERT(!semantic_activation_modifier_policy(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
			| CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1
			| (UINT64_C(1) << 63),
		12, false));
	UT_ASSERT(!semantic_activation_modifier_policy(UINT64_C(1) << 63, 9, false));
}

UT_TEST(test_86_r4a_snapshot_is_fixed_false)
{
	ClusterR4PrerequisiteSnapshot snapshot = cluster_undo_block0_r4_prerequisite_snapshot();

	UT_ASSERT(!snapshot.ready);
	UT_ASSERT_EQ(snapshot.status, CLUSTER_R4_PREREQUISITE_RF_DEFERRED);
	UT_ASSERT_EQ(snapshot.reserved0[0] | snapshot.reserved0[1] | snapshot.reserved0[2], 0);
	UT_ASSERT_EQ(snapshot.target_node_id, -1);
	UT_ASSERT_EQ((unsigned long long)snapshot.jcmk_generation, 0ULL);
	UT_ASSERT_EQ((unsigned long long)snapshot.grammar_fingerprint, 0ULL);
}

UT_TEST(test_87_readiness_adapter_returns_rf_deferred)
{
	ClusterSemanticActivationRefusal refusal;

	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(r4_pre_prepare_readiness(19, &refusal), CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
}

UT_TEST(test_88_readiness_adapter_names_r4_feature)
{
	ClusterSemanticActivationRefusal refusal;

	memset(&refusal, 0, sizeof(refusal));
	(void)r4_pre_prepare_readiness(19, &refusal);
	UT_ASSERT_EQ(refusal.feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
}

UT_TEST(test_89_readiness_adapter_preserves_expected_generation)
{
	ClusterSemanticActivationRefusal refusal;

	memset(&refusal, 0, sizeof(refusal));
	(void)r4_pre_prepare_readiness(19, &refusal);
	UT_ASSERT_EQ(refusal.expected_generation, 19);
}

static ClusterR4PrerequisiteSnapshot
valid_r4a_ready_snapshot(void)
{
	ClusterR4PrerequisiteSnapshot snapshot;

	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.status = CLUSTER_R4_PREREQUISITE_R4A_READY;
	snapshot.ready = true;
	snapshot.target_node_id = 3;
	snapshot.episode_state_generation = UINT32_C(17);
	snapshot.jcmk_generation = UINT64_C(41);
	snapshot.request_nonce = UINT64_C(0x123456789abcdef0);
	snapshot.old_admitted_incarnation = UINT64_C(9001);
	snapshot.fresh_incarnation = UINT64_C(9002);
	snapshot.committed_epoch = UINT64_C(71);
	snapshot.grammar_fingerprint
		= CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT;
	return snapshot;
}

UT_TEST(test_89d_local_ready_alone_cannot_enter_prepare)
{
	ClusterR4PrerequisiteSnapshot snapshot = valid_r4a_ready_snapshot();

	test_gate_reset();
	test_r4a_snapshot = snapshot;
	UT_ASSERT_EQ(r4_pre_prepare_readiness(19, NULL),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
}

UT_TEST(test_89e_malformed_local_ready_remains_typed_deferred)
{
	ClusterSemanticActivationRefusal refusal;

	test_gate_reset();
	test_r4a_snapshot = valid_r4a_ready_snapshot();
	test_r4a_snapshot.reserved0[1] = 1;
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(r4_pre_prepare_readiness(19, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
}

static ClusterReplacementCommitMarkerV3
valid_d13_admitted_marker(void)
{
	ClusterReplacementCommitMarkerV3 marker;

	memset(&marker, 0, sizeof(marker));
	marker.magic = CLUSTER_JCMK_MAGIC;
	marker.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	marker.target_node_id = 3;
	marker.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED;
	marker.generation = 42;
	marker.old_admitted_incarnation = UINT64_C(9001);
	marker.fresh_incarnation = UINT64_C(9002);
	marker.baseline_epoch = UINT64_C(70);
	marker.reserved_or_committed_epoch = UINT64_C(71);
	marker.request_nonce = UINT64_C(0x123456789abcdef0);
	marker.expected_purge_survivors[0] = UINT8_C(0x05);
	marker.grammar_fingerprint = UINT64_C(0x8e0dae5b428905e4);
	marker.ready_state_generation = UINT32_C(17);
	return marker;
}

static ClusterReplacementEpisode
valid_d13_admitted_episode(void)
{
	ClusterReplacementEpisode episode;

	memset(&episode, 0, sizeof(episode));
	episode.request_nonce = UINT64_C(0x123456789abcdef0);
	episode.baseline_epoch = UINT64_C(70);
	episode.reserved_or_committed_epoch = UINT64_C(71);
	episode.old_admitted_incarnation = UINT64_C(9001);
	episode.fresh_incarnation = UINT64_C(9002);
	episode.grammar_fingerprint = UINT64_C(0x8e0dae5b428905e4);
	episode.expected_survivors[0] = UINT8_C(0x05);
	episode.target_node_id = 3;
	episode.coordinator_node_id = 0;
	episode.state_generation = UINT32_C(17);
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_ADMITTED;
	episode.readiness_flags = CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK;
	return episode;
}

UT_TEST(test_89a_d13_prepare_basis_accepts_only_exact_admitted_ready_lineage)
{
	ClusterReplacementCommitMarkerV3 marker = valid_d13_admitted_marker();
	ClusterReplacementEpisode episode = valid_d13_admitted_episode();

	UT_ASSERT(semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(NULL, &episode));
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, NULL));
}

UT_TEST(test_89b_d13_prepare_basis_requires_admitted_ready_polarity)
{
	ClusterReplacementCommitMarkerV3 marker = valid_d13_admitted_marker();
	ClusterReplacementEpisode episode = valid_d13_admitted_episode();

	marker.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	marker = valid_d13_admitted_marker();
	marker.ready_state_generation = 0;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	marker = valid_d13_admitted_marker();
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.readiness_flags &= (uint8)~CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.reserved[1] = 1;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
}

UT_TEST(test_89c_d13_prepare_basis_rejects_generation_or_identity_drift)
{
	ClusterReplacementCommitMarkerV3 marker = valid_d13_admitted_marker();
	ClusterReplacementEpisode episode = valid_d13_admitted_episode();

	episode.state_generation++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.target_node_id++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.request_nonce++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.old_admitted_incarnation++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.fresh_incarnation++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.baseline_epoch++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.reserved_or_committed_epoch++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.expected_survivors[1] = UINT8_C(1);
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.grammar_fingerprint++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
}

UT_TEST(test_89f_positive_ready_stays_deferred_until_full_d13_conjunction)
{
	ClusterSemanticActivationRefusal refusal;

	test_gate_reset();
	test_r4a_snapshot = valid_r4a_ready_snapshot();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(r4_pre_prepare_readiness(19, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	test_gate_reset();
}

UT_TEST(test_89g_d13_current_coordinator_handoff_consumes_exact_admitted_basis)
{
	test_gate_reset();
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	UT_ASSERT(semantic_activation_r4_current_admitted_basis());
	UT_ASSERT_EQ(test_admitted_snapshot_calls, 1);

	test_admitted_snapshot_marker.ready_state_generation++;
	UT_ASSERT(!semantic_activation_r4_current_admitted_basis());
	UT_ASSERT_EQ(test_admitted_snapshot_calls, 2);
}

UT_TEST(test_89h_pre_prepare_consumes_durable_admitted_not_ready_getter)
{
	ClusterSemanticActivationRefusal refusal;

	test_gate_reset();
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(r4_pre_prepare_readiness(19, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(test_admitted_snapshot_calls, 1);
	UT_ASSERT_EQ(test_r4a_snapshot_calls, 0);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(refusal.feature_bit,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(refusal.expected_generation, 19);
	test_gate_reset();
}

UT_TEST(test_89h1_pre_prepare_refuses_when_ctrc_storage_is_not_ready)
{
	ClusterSemanticActivationRefusal refusal;

	test_gate_reset();
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	test_ctrc_shmem_is_ready = false;
	MemSet(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(r4_pre_prepare_readiness(19, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	test_gate_reset();
}

UT_TEST(test_89i_d13_invalidator_rescan_accepts_only_same_settled_closed_head)
{
	ClusterReplacementEpisode episode = valid_d13_admitted_episode();
	ClusterReplacementCommitMarkerV3 marker = valid_d13_admitted_marker();
	ClusterQvotecMailboxCompletion completion;
	ClusterEpochAuthorityValue head;
	ClusterEpochBallotId ballot;

	memset(&completion, 0, sizeof(completion));
	completion.request_seq = UINT64_C(2);
	completion.result = CLUSTER_QVOTEC_MAILBOX_CHOSEN;
	completion.observed_disk_bitmap = UINT8_C(0x01);
	completion.actor_phase = CLUSTER_QVOTEC_ACTOR_RECOVER_SCAN_B;

	memset(&head, 0, sizeof(head));
	head.value_version = CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION;
	head.transition = CLUSTER_EPOCH_AUTHORITY_COMMIT_CLOSED;
	head.event_kind = CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT;
	head.request_origin_node = episode.target_node_id;
	head.target_node_id = episode.target_node_id;
	head.authority_generation = UINT64_C(103);
	head.baseline_epoch = episode.baseline_epoch;
	head.reserved_epoch = episode.reserved_or_committed_epoch;
	head.old_incarnation = episode.old_admitted_incarnation;
	head.fresh_incarnation = episode.fresh_incarnation;
	head.request_nonce = episode.request_nonce;
	memcpy(head.authority_member_bitmap, episode.expected_survivors,
		   sizeof(head.authority_member_bitmap));
	head.event_subject_bitmap[episode.target_node_id / 8]
		= (uint8)(1u << (episode.target_node_id % 8));
	head.grammar_fingerprint = episode.grammar_fingerprint;
	memset(head.predecessor_digest, 0x5a, sizeof(head.predecessor_digest));
	UT_ASSERT(cluster_epoch_authority_value_encode(
		&head, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
		completion.completion_value));

	memset(&ballot, 0, sizeof(ballot));
	ballot.counter = UINT64_C(7);
	ballot.proposer_node_id = 1;
	ballot.proposer_admitted_incarnation = UINT64_C(111);
	ballot.nonce = UINT64_C(0xabcdef);
	UT_ASSERT(cluster_epoch_ballot_id_encode(
		&ballot, completion.completion_ballot));

	UT_ASSERT(semantic_activation_r4_invalidator_rescan_matches(
		&completion, &marker, &episode));
	completion.result = CLUSTER_QVOTEC_MAILBOX_ADOPTED_OTHER;
	UT_ASSERT(!semantic_activation_r4_invalidator_rescan_matches(
		&completion, &marker, &episode));
	completion.result = CLUSTER_QVOTEC_MAILBOX_CHOSEN;
	head.fresh_incarnation++;
	UT_ASSERT(cluster_epoch_authority_value_encode(
		&head, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
		completion.completion_value));
	UT_ASSERT(!semantic_activation_r4_invalidator_rescan_matches(
		&completion, &marker, &episode));
}

UT_TEST(test_90_descriptor_uses_the_only_r4a_adapter)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();

	UT_ASSERT(descriptor->pre_prepare_readiness == r4_pre_prepare_readiness);
}

UT_TEST(test_90aa_r4_logical_zero_requires_exact_closed_source_gate)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();
	ClusterSemanticZeroProof proof = {
		.record_generation = UINT64_MAX,
		.debt_count = UINT64_MAX,
		.sample_digest = UINT64_MAX,
	};

	test_gate_reset();
	test_gate_publish(2, 0, 24, test_current_epoch, false);
	UT_ASSERT_EQ(descriptor->source_logical_debt_zero(24, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(proof.record_generation, UINT64_C(0));
	UT_ASSERT_EQ(proof.debt_count, UINT64_C(0));
	UT_ASSERT_EQ(proof.sample_digest, UINT64_C(0));
	UT_ASSERT_EQ(descriptor->source_logical_debt_zero(24, NULL),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
}

UT_TEST(test_90ab_r4_logical_zero_refuses_live_source_debt)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();
	ClusterSemanticZeroProof proof;

	test_gate_reset();
	test_gate_publish(2, 0, 24, test_current_epoch, true);
	pg_atomic_write_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0), 1);
	UT_ASSERT_EQ(descriptor->source_logical_debt_zero(24, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO);
	UT_ASSERT_EQ(proof.record_generation, UINT64_C(0));
	UT_ASSERT_EQ(proof.debt_count, UINT64_C(0));
	UT_ASSERT_EQ(proof.sample_digest, UINT64_C(0));
}

UT_TEST(test_90ac_r4_logical_zero_rechecks_gate_before_proof)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();
	ClusterSemanticZeroProof proof;

	test_gate_reset();
	test_gate_publish(2, 0, 24, test_current_epoch, true);
	test_advance_epoch_on_read_barrier = 3;
	UT_ASSERT_EQ(descriptor->source_logical_debt_zero(24, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(proof.record_generation, UINT64_C(0));
	UT_ASSERT_EQ(proof.debt_count, UINT64_C(0));
	UT_ASSERT_EQ(proof.sample_digest, UINT64_C(0));

	test_gate_reset();
	test_gate_publish(2, 0, 24, test_current_epoch, true);
	UT_ASSERT_EQ(descriptor->source_logical_debt_zero(24, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(proof.record_generation, UINT64_C(24));
	UT_ASSERT_EQ(proof.debt_count, UINT64_C(0));
	UT_ASSERT_EQ(proof.sample_digest, UINT64_C(0));
}

UT_TEST(test_90a_r4_transport_zero_requires_exact_closed_gate)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();
	ClusterSemanticZeroProof proof = {
		.record_generation = UINT64_MAX,
		.debt_count = UINT64_MAX,
		.sample_digest = UINT64_MAX,
	};

	test_gate_reset();
	test_gate_publish(2, 0, 24, test_current_epoch, false);
	UT_ASSERT_EQ(descriptor->source_transport_zero(24, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(proof.record_generation, UINT64_C(0));
	UT_ASSERT_EQ(proof.debt_count, UINT64_C(0));
	UT_ASSERT_EQ(proof.sample_digest, UINT64_C(0));
	UT_ASSERT_EQ(test_drain_request_calls, 0);
	UT_ASSERT_EQ(test_reclaim_calls, 0);
	UT_ASSERT_EQ(test_route_purge_calls, 0);
	UT_ASSERT_EQ(descriptor->source_transport_zero(24, NULL),
				 CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
}

UT_TEST(test_90b_r4_transport_zero_refuses_target_debt_and_missing_drain)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();
	ClusterSemanticZeroProof proof;

	test_gate_reset();
	test_gate_publish(2, 0, 24, test_current_epoch, true);
	pg_atomic_write_u32(test_gate_inflight(CLUSTER_SEMANTIC_TARGET_SIDE, 0), 1);
	UT_ASSERT_EQ(descriptor->source_transport_zero(24, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO);
	UT_ASSERT_EQ(test_drain_request_calls, 0);

	pg_atomic_write_u32(test_gate_inflight(CLUSTER_SEMANTIC_TARGET_SIDE, 0), 0);
	test_drain_request_succeeds = false;
	UT_ASSERT_EQ(descriptor->source_transport_zero(24, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO);
	UT_ASSERT_EQ(test_drain_request_calls, 1);
	UT_ASSERT_EQ(test_drain_request_generation, UINT64_C(24));
	UT_ASSERT_EQ(test_lms_wakeup_calls, 0);
	UT_ASSERT_EQ(test_reclaim_calls, 0);
}

UT_TEST(test_90c_r4_transport_zero_refuses_partial_close_conjunction)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();
	ClusterSemanticZeroProof proof;

	test_gate_reset();
	test_gate_publish(2, 0, 24, test_current_epoch, true);
	UT_ASSERT_EQ(descriptor->source_transport_zero(24, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO);
	UT_ASSERT_EQ(test_lms_wakeup_calls, 1);
	UT_ASSERT_EQ(test_lms_wakeup_worker, 0);
	UT_ASSERT_EQ(test_reclaim_calls, 1);
	UT_ASSERT_EQ(test_reclaim_worker_incarnation, UINT64_C(8));
	UT_ASSERT_EQ(test_reclaim_generation, UINT64_C(24));
	UT_ASSERT_EQ(test_route_purge_calls, 0);
	UT_ASSERT_EQ(proof.record_generation, UINT64_C(0));
	UT_ASSERT_EQ(proof.debt_count, UINT64_C(0));
	UT_ASSERT_EQ(proof.sample_digest, UINT64_C(0));

	test_reclaim_succeeds = true;
	test_route_count = 1;
	UT_ASSERT_EQ(descriptor->source_transport_zero(24, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO);
	UT_ASSERT_EQ(test_route_purge_calls, 1);
	UT_ASSERT_EQ(test_route_count_calls, 1);
	UT_ASSERT_EQ(test_requester_count_calls, 0);

	test_route_count = 0;
	test_requester_count = 1;
	UT_ASSERT_EQ(descriptor->source_transport_zero(24, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO);
	UT_ASSERT_EQ(test_requester_count_calls, 1);
	UT_ASSERT_EQ(proof.record_generation, UINT64_C(0));
}

UT_TEST(test_90d_r4_transport_zero_publishes_only_full_zero_proof)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();
	ClusterSemanticZeroProof proof = {
		.record_generation = UINT64_MAX,
		.debt_count = UINT64_MAX,
		.sample_digest = UINT64_MAX,
	};

	test_gate_reset();
	test_gate_publish(2, 0, 24, test_current_epoch, true);
	test_reclaim_succeeds = true;
	test_route_purge_result = 3;
	UT_ASSERT_EQ(descriptor->source_transport_zero(24, &proof),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(test_drain_request_calls, 1);
	UT_ASSERT_EQ(test_drain_request_generation, UINT64_C(24));
	UT_ASSERT_EQ(test_lms_wakeup_calls, 1);
	UT_ASSERT_EQ(test_reclaim_calls, 1);
	UT_ASSERT_EQ(test_route_purge_calls, 1);
	UT_ASSERT_EQ(test_route_count_calls, 1);
	UT_ASSERT_EQ(test_requester_count_calls, 1);
	UT_ASSERT_EQ(proof.record_generation, UINT64_C(24));
	UT_ASSERT_EQ(proof.debt_count, UINT64_C(0));
	UT_ASSERT_EQ(proof.sample_digest, UINT64_C(0));
}

UT_TEST(test_91_preflight_refusal_is_before_every_mutation)
{
	ClusterSemanticActivationRefusal refusal;
	uint32 effects = UINT32_MAX;

	UT_ASSERT_EQ(semantic_activation_preflight(CLUSTER_SEMANTIC_ENABLE_ALL, 0, &refusal, &effects),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(effects, SEMANTIC_ACTIVATION_EFFECT_NONE);
}

UT_TEST(test_92_preflight_refusal_names_condition_feature)
{
	ClusterSemanticActivationRefusal refusal;
	uint32 effects = UINT32_MAX;

	(void)semantic_activation_preflight(CLUSTER_SEMANTIC_ENABLE_ALL, 23, &refusal, &effects);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(refusal.feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(refusal.expected_generation, 23);
}

UT_TEST(test_93_preflight_rejects_bad_action_without_effects)
{
	ClusterSemanticActivationRefusal refusal;
	uint32 effects = UINT32_MAX;

	UT_ASSERT_EQ(
		semantic_activation_preflight((ClusterSemanticActivationAction)4, 0, &refusal, &effects),
		CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(effects, SEMANTIC_ACTIVATION_EFFECT_NONE);
}

UT_TEST(test_93a_activation_actor_effect_ownership_is_exact)
{
	UT_ASSERT(semantic_activation_actor_effect_allowed(
		SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY,
		SEMANTIC_ACTIVATION_EFFECT_REQUEST_PUBLICATION));
	UT_ASSERT(!semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY,
										SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE));
	UT_ASSERT(semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
									  SEMANTIC_ACTIVATION_EFFECT_SOURCE_CLOSE));
	UT_ASSERT(semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
									  SEMANTIC_ACTIVATION_EFFECT_TARGET_OPEN));
	UT_ASSERT(semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
									  SEMANTIC_ACTIVATION_EFFECT_ACK_MUTATION));
	UT_ASSERT(semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
									  SEMANTIC_ACTIVATION_EFFECT_CONTROL_WIRE));
	UT_ASSERT(!semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
									   SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE));
	UT_ASSERT(semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_QVOTEC,
									  SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE));
	UT_ASSERT(!semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_QVOTEC,
									   SEMANTIC_ACTIVATION_EFFECT_ACK_MUTATION));
	UT_ASSERT(!semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_LMS,
									   SEMANTIC_ACTIVATION_EFFECT_DATA_WIRE));
	UT_ASSERT(!semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_DATA,
									   SEMANTIC_ACTIVATION_EFFECT_DATA_WIRE));
	UT_ASSERT(!semantic_activation_actor_effect_allowed(
		SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY,
		(SemanticActivationEffect)(SEMANTIC_ACTIVATION_EFFECT_REQUEST_PUBLICATION
							   | SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE)));
}

UT_TEST(test_93b_activation_mailbox_route_has_no_owner_bypass)
{
	UT_ASSERT(semantic_activation_actor_edge_allowed(
		SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY, SEMANTIC_ACTIVATION_ACTOR_LMON));
	UT_ASSERT(semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
										SEMANTIC_ACTIVATION_ACTOR_QVOTEC));
	UT_ASSERT(!semantic_activation_actor_edge_allowed(
		SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY, SEMANTIC_ACTIVATION_ACTOR_QVOTEC));
	UT_ASSERT(!semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY,
										 SEMANTIC_ACTIVATION_ACTOR_LMS));
	UT_ASSERT(!semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
										 SEMANTIC_ACTIVATION_ACTOR_DATA));
	UT_ASSERT(!semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_QVOTEC,
										 SEMANTIC_ACTIVATION_ACTOR_LMON));
}

UT_TEST(test_93c_utility_mailbox_preserves_exact_owner_tuple_and_completion)
{
	SemanticActivationUtilityRequest request;
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;
	uint64 blocked_seq = 0;

	test_gate_reset();
	memset(&request, 0, sizeof(request));
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, UINT64_C(0x11), UINT64_C(0x22),
		UINT64_C(0x33), UINT64_C(41), &request_seq));
	UT_ASSERT(request_seq != 0);
	UT_ASSERT(!semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_DISABLE_ALL, 0, 0, 0, 0, &blocked_seq));
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&request));
	UT_ASSERT_EQ(request.request_seq, request_seq);
	UT_ASSERT_EQ(request.action, CLUSTER_SEMANTIC_ENABLE_ALL);
	UT_ASSERT_EQ(request.source_feature_bitmap, UINT64_C(0x11));
	UT_ASSERT_EQ(request.target_feature_bitmap, UINT64_C(0x22));
	UT_ASSERT_EQ(request.rollback_feature_bitmap, UINT64_C(0x33));
	UT_ASSERT_EQ(request.expected_record_generation, UINT64_C(41));
	UT_ASSERT_EQ(pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq), UINT64_C(0));
	UT_ASSERT(!semantic_activation_utility_mailbox_complete(
		request_seq + 1, CLUSTER_SEMANTIC_ACTIVATION_OK, 0, 41));
	UT_ASSERT(semantic_activation_utility_mailbox_complete(
		request_seq, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 41));
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(refusal.feature_bit,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(refusal.expected_generation, UINT64_C(41));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
}

UT_TEST(test_93d_formation_lmon_alone_consumes_utility_request)
{
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;

	test_gate_reset();
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0, &request_seq));
	cluster_semantic_activation_lmon_tick();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(refusal.feature_bit,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(refusal.expected_generation, UINT64_C(0));
	UT_ASSERT_EQ(test_admitted_snapshot_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq), UINT64_C(0));
	test_gate_reset();
}

UT_TEST(test_93da_coordinator_begins_exact_four_node_sample_round)
{
	SemanticActivationUtilityRequest pending_request;
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationAckWireV1 message;
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;
	int node;

	test_gate_reset();
	cluster_node_id = 0;
	test_gate_publish(2, 0, 7, test_current_epoch, false);
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_generation = 19;
	for (node = 0; node < CLUSTER_MAX_NODES; node++)
		test_send_results[node] = CLUSTER_IC_SEND_DONE;

	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 7,
		&request_seq));
	cluster_semantic_activation_lmon_tick();

	memset(&pending_request, 0, sizeof(pending_request));
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending_request));
	UT_ASSERT_EQ(pending_request.request_seq, request_seq);
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(table.flags, UINT32_C(0));
	UT_ASSERT_EQ(table.coordinator_node, UINT32_C(0));
	UT_ASSERT_EQ(table.round_nonce, request_seq);
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(table.expected_members_hi, UINT64_C(0));
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x01));
	UT_ASSERT_EQ(table.observed_members_hi, UINT64_C(0));
	UT_ASSERT_EQ(table.transition_epoch, test_current_epoch);
	UT_ASSERT_EQ(table.record_generation, UINT64_C(8));
	UT_ASSERT_EQ(table.source_feature_bitmap, UINT64_C(0));
	UT_ASSERT_EQ(table.target_feature_bitmap,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(table.rollback_feature_bitmap, UINT64_C(0));
	UT_ASSERT_EQ(table.capability_sample_digest, UINT64_C(0));
	UT_ASSERT_EQ(table.observed[0].node_id, UINT32_C(0));
	UT_ASSERT_EQ(table.observed[0].boot_id,
				 test_qvotec_self_incarnation);
	UT_ASSERT_EQ(table.observed[0].admitted_incarnation,
				 test_qvotec_self_incarnation);
	UT_ASSERT_EQ(table.observed[0].capability_word,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS);
	UT_ASSERT_EQ(table.observed[0].transition_epoch,
				 test_current_epoch);
	UT_ASSERT_EQ(table.observed[0].record_generation, UINT64_C(8));
	UT_ASSERT_EQ(test_send_calls[0], 0);
	for (node = 1; node < 4; node++) {
		UT_ASSERT_EQ(test_send_calls[node], 2);
		UT_ASSERT_EQ(test_send_history_msg_types[node][0],
					 PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1);
		UT_ASSERT_EQ(test_send_history_payload_lengths[node][0],
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES);
		memset(&message, 0, sizeof(message));
		UT_ASSERT(cluster_semantic_activation_ack_wire_decode(
			test_send_history_payloads[node][0], &message));
		UT_ASSERT_EQ(message.kind,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST);
		UT_ASSERT_EQ(message.stage,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
		UT_ASSERT_EQ(message.result,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST);
		UT_ASSERT_EQ(message.reason, UINT32_C(0));
		UT_ASSERT_EQ(message.coordinator_node, UINT32_C(0));
		UT_ASSERT_EQ(message.member_node, (uint32)node);
		UT_ASSERT_EQ(message.transition_epoch, test_current_epoch);
		UT_ASSERT_EQ(message.record_generation, UINT64_C(8));
		UT_ASSERT_EQ(message.round_nonce, request_seq);
		UT_ASSERT_EQ(message.source_feature_bitmap, UINT64_C(0));
		UT_ASSERT_EQ(message.target_feature_bitmap,
					 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
		UT_ASSERT_EQ(message.rollback_feature_bitmap, UINT64_C(0));
		UT_ASSERT_EQ(message.admitted_members_lo, UINT64_C(0x0f));
		UT_ASSERT_EQ(message.admitted_members_hi, UINT64_C(0));
		UT_ASSERT_EQ(message.capability_sample_digest, UINT64_C(0));
		UT_ASSERT_EQ(message.boot_id, UINT64_C(0));
		UT_ASSERT_EQ(message.admitted_incarnation, UINT64_C(0));
		UT_ASSERT_EQ(message.capability_word, UINT32_C(0));

		UT_ASSERT_EQ(test_send_history_msg_types[node][1],
					 PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1);
		UT_ASSERT_EQ(test_send_history_payload_lengths[node][1],
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES);
		memset(&message, 0, sizeof(message));
		UT_ASSERT(cluster_semantic_activation_ack_wire_decode(
			test_send_history_payloads[node][1], &message));
		UT_ASSERT_EQ(message.kind,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK);
		UT_ASSERT_EQ(message.stage,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
		UT_ASSERT_EQ(message.result,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK);
		UT_ASSERT_EQ(message.reason,
					 CLUSTER_SEMANTIC_ACTIVATION_OK);
		UT_ASSERT_EQ(message.coordinator_node, UINT32_C(0));
		UT_ASSERT_EQ(message.member_node, UINT32_C(0));
		UT_ASSERT_EQ(message.transition_epoch, test_current_epoch);
		UT_ASSERT_EQ(message.record_generation, UINT64_C(8));
		UT_ASSERT_EQ(message.round_nonce, request_seq);
		UT_ASSERT_EQ(message.source_feature_bitmap, UINT64_C(0));
		UT_ASSERT_EQ(message.target_feature_bitmap,
					 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
		UT_ASSERT_EQ(message.rollback_feature_bitmap, UINT64_C(0));
		UT_ASSERT_EQ(message.admitted_members_lo, UINT64_C(0x0f));
		UT_ASSERT_EQ(message.admitted_members_hi, UINT64_C(0));
		UT_ASSERT_EQ(message.capability_sample_digest, UINT64_C(0));
		UT_ASSERT_EQ(message.boot_id, test_qvotec_self_incarnation);
		UT_ASSERT_EQ(message.admitted_incarnation,
					 test_qvotec_self_incarnation);
		UT_ASSERT_EQ(message.capability_word,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS);
	}
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending_request));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
	for (node = 1; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 2);
	test_gate_reset();
}

UT_TEST(test_93da0_early_sample_without_common_view_sends_one_directed_refusal)
{
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationAckWireV1 message;
	ClusterSemanticActivationAckWireV1 sent;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	int node;

	test_gate_reset();
	cluster_node_id = 3;
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	test_membership_snapshot_valid = false;
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	test_send_results[0] = CLUSTER_IC_SEND_DONE;

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 0;
	message.member_node = 3;
	message.transition_epoch = test_current_epoch;
	message.record_generation = 1;
	message.round_nonce = UINT64_C(404);
	message.target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	message.admitted_members_lo = UINT64_C(0x0f);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = 0;
	envelope.dest_node_id = 3;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(payload);

	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(semantic_activation_ack_local_pending_send.message.result,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED);
	UT_ASSERT(!semantic_activation_ack_local_pending_send.invalidated);
	UT_ASSERT_EQ(
		semantic_activation_ack_local_pending_send.refusal_connection_generation,
		UINT32_C(19));
	UT_ASSERT_EQ(semantic_activation_ack_local_pending_send.pending_members_lo,
				 UINT64_C(0));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], node == 0 ? 1 : 0);
	UT_ASSERT(cluster_semantic_activation_ack_wire_decode(
		test_send_payloads[0], &sent));
	UT_ASSERT_EQ(sent.kind, CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK);
	UT_ASSERT_EQ(sent.stage, message.stage);
	UT_ASSERT_EQ(sent.result,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED);
	UT_ASSERT_EQ(sent.reason, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(sent.coordinator_node, UINT32_C(0));
	UT_ASSERT_EQ(sent.member_node, UINT32_C(3));
	UT_ASSERT_EQ(sent.transition_epoch, message.transition_epoch);
	UT_ASSERT_EQ(sent.record_generation, message.record_generation);
	UT_ASSERT_EQ(sent.round_nonce, message.round_nonce);
	UT_ASSERT_EQ(sent.source_feature_bitmap, message.source_feature_bitmap);
	UT_ASSERT_EQ(sent.target_feature_bitmap, message.target_feature_bitmap);
	UT_ASSERT_EQ(sent.rollback_feature_bitmap,
				 message.rollback_feature_bitmap);
	UT_ASSERT_EQ(sent.admitted_members_lo, message.admitted_members_lo);
	UT_ASSERT_EQ(sent.admitted_members_hi, message.admitted_members_hi);
	UT_ASSERT_EQ(sent.capability_sample_digest,
				 message.capability_sample_digest);
	UT_ASSERT_EQ(sent.boot_id, UINT64_C(0));
	UT_ASSERT_EQ(sent.admitted_incarnation, UINT64_C(0));
	UT_ASSERT_EQ(sent.capability_word, UINT32_C(0));

	/* Transport-admitted duplicate evidence is idempotent. */
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(test_send_calls[0], 1);
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0));
	test_gate_reset();
}

UT_TEST(test_93da0_exact_refusal_completes_active_utility_as_deferred)
{
	const uint64 r4 = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	ClusterSemanticActivationAckTableV1 *table;
	ClusterSemanticActivationAckWireV1 message;
	ClusterSemanticActivationRefusal refusal;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	uint64 request_seq = 0;

	test_gate_reset();
	cluster_node_id = 0;
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0, r4, 0, 0, &request_seq));

	table = SemanticActivationAckTable;
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	table->coordinator_node = 0;
	table->round_nonce = request_seq;
	table->expected_members_lo = UINT64_C(0x0f);
	table->observed_members_lo = UINT64_C(0x01);
	table->transition_epoch = test_current_epoch;
	table->record_generation = 1;
	table->target_feature_bitmap = r4;

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED;
	message.reason = CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
	message.coordinator_node = 0;
	message.member_node = 3;
	message.transition_epoch = test_current_epoch;
	message.record_generation = 1;
	message.round_nonce = request_seq + 1;
	message.target_feature_bitmap = r4;
	message.admitted_members_lo = UINT64_C(0x0f);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = 3;
	envelope.dest_node_id = 0;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(payload);

	/* A wrong-round refusal is stale and cannot complete or erase the round. */
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(table->expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));

	message.round_nonce = request_seq;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(table->expected_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0));
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(refusal.feature_bit, r4);
	test_gate_reset();
}

UT_TEST(test_93da1_member_retains_sample_ack_until_exact_request_arrives)
{
	const uint64 system_identifier = UINT64_C(0x8070605040302010);
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationAckWireV1 message;
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	int node;

	test_gate_reset();
	cluster_node_id = 3;
	test_gate_publish(2, 0, 7, test_current_epoch, false);
	test_initial_clean_snapshot_valid = true;
	test_initial_clean_snapshot.formation_marker_generation = 3;
	test_initial_clean_snapshot.formation_epoch = test_current_epoch;
	test_initial_clean_snapshot.members_lo = UINT64_C(0x0f);
	test_initial_clean_snapshot.arbiter_node = 0;
	test_system_identifier = system_identifier;
	cluster_shared_data_dir = "/cluster-share";
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 4; node++) {
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;
		test_initial_clean_snapshot.admitted_incarnation[node]
			= test_remote_admitted_incarnations[node];
	}
	test_initial_clean_snapshot.admitted_incarnation[cluster_node_id]
		= test_qvotec_self_incarnation;
	test_initial_clean_snapshot.arbiter_incarnation
		= test_initial_clean_snapshot.admitted_incarnation[0];
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	memset(descriptor.root_uuid, 0x6b, sizeof(descriptor.root_uuid));
	descriptor.namespace_id = 1;
	descriptor.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(
		&descriptor, test_pgrd_candidate));
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
	UT_ASSERT(semantic_activation_pgrd_snapshot_publish(
		test_pgrd_candidate));
	/* Node 1 can finish the directed SAMPLE request and fan out its ACK
	 * before node 3 receives the coordinator's directed request.  A
	 * non-initial generation remains stale even with the same wire shape. */
	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	message.reason = CLUSTER_SEMANTIC_ACTIVATION_OK;
	message.coordinator_node = 0;
	message.member_node = 1;
	message.transition_epoch = test_current_epoch;
	message.record_generation = 8;
	message.round_nonce = UINT64_C(77);
	message.target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	message.admitted_members_lo = UINT64_C(0x0f);
	message.boot_id = test_remote_admitted_incarnations[1];
	message.admitted_incarnation = message.boot_id;
	message.capability_word = test_peer_capability_word;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = 1;
	envelope.dest_node_id = 3;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&envelope, payload);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(semantic_activation_ack_local_stage_ahead.count,
				 UINT32_C(0));

	test_gate_publish(4, 0, 0, test_current_epoch, false);
	message.record_generation = 1;
	message.round_nonce = UINT64_C(78);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	cluster_semantic_activation_ack_handler(&envelope, payload);
	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ(semantic_activation_ack_local_stage_ahead.count,
				 UINT32_C(1));
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0));

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 0;
	message.member_node = 3;
	message.transition_epoch = test_current_epoch;
	message.record_generation = 1;
	message.round_nonce = UINT64_C(78);
	message.target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	message.admitted_members_lo = UINT64_C(0x0f);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	envelope.source_node_id = 0;
	cluster_semantic_activation_ack_handler(&envelope, payload);
	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ(semantic_activation_ack_local_stage_ahead.count,
				 UINT32_C(0));
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x0a));
	UT_ASSERT(semantic_activation_ack_matches(
		&table.observed[1],
		&(SemanticActivationAckTuple){
			.node_id = 1,
			.boot_id = test_remote_admitted_incarnations[1],
			.admitted_incarnation = test_remote_admitted_incarnations[1],
			.control_connection_generation = 19,
			.capability_word = test_peer_capability_word,
			.capability_generation = 19,
			.transition_epoch = test_current_epoch,
			.record_generation = 1,
		}));
	test_gate_reset();
}

UT_TEST(test_93da2_member_retains_next_round_sample_ack_until_exact_request_arrives)
{
	const uint32 resource_x_caps
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
		  | PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	const uint64 r4 = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	const uint64 target
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	ClusterSemanticActivationAckTableV1 *table;
	ClusterSemanticActivationAckWireV1 message;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	int node;

	test_gate_reset();
	cluster_node_id = 3;
	test_gate_publish(2, r4, 1, test_current_epoch, false);
	test_local_capability_word = resource_x_caps;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word = resource_x_caps;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 4; node++)
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;

	table = SemanticActivationAckTable;
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->coordinator_node = 0;
	table->round_nonce = UINT64_C(77);
	table->expected_members_lo = UINT64_C(0x0f);
	table->observed_members_lo = UINT64_C(0x0f);
	table->transition_epoch = test_current_epoch;
	table->record_generation = 1;
	table->target_feature_bitmap = r4;
	for (node = 0; node < 4; node++) {
		if (node == cluster_node_id) {
			UT_ASSERT(semantic_activation_ack_self_tuple(
				node, resource_x_caps, test_current_epoch, 1,
				&table->expected[node]));
		} else {
			table->expected[node].node_id = (uint32)node;
			table->expected[node].boot_id
				= test_remote_admitted_incarnations[node];
			table->expected[node].admitted_incarnation
				= test_remote_admitted_incarnations[node];
			table->expected[node].control_connection_generation = 19;
			table->expected[node].capability_word = resource_x_caps;
			table->expected[node].capability_generation = 19;
			table->expected[node].transition_epoch = test_current_epoch;
			table->expected[node].record_generation = 1;
		}
		table->observed[node] = table->expected[node];
	}
	table->capability_sample_digest = UINT64_C(0x3141592653589793);

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	message.reason = CLUSTER_SEMANTIC_ACTIVATION_OK;
	message.coordinator_node = 0;
	message.member_node = 1;
	message.transition_epoch = test_current_epoch;
	message.record_generation = 2;
	message.round_nonce = UINT64_C(78);
	message.source_feature_bitmap = r4;
	message.target_feature_bitmap = target;
	message.admitted_members_lo = UINT64_C(0x0f);
	message.boot_id = test_remote_admitted_incarnations[1];
	message.admitted_incarnation = message.boot_id;
	message.capability_word = resource_x_caps;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = 1;
	envelope.dest_node_id = 3;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&envelope, payload);
	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ(semantic_activation_ack_local_stage_ahead.count,
				 UINT32_C(1));
	UT_ASSERT(semantic_activation_ack_table_snapshot(table));
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED);
	UT_ASSERT_EQ(table->record_generation, UINT64_C(1));

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 0;
	message.member_node = 3;
	message.transition_epoch = test_current_epoch;
	message.record_generation = 2;
	message.round_nonce = UINT64_C(78);
	message.source_feature_bitmap = r4;
	message.target_feature_bitmap = target;
	message.admitted_members_lo = UINT64_C(0x0f);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	envelope.source_node_id = 0;
	cluster_semantic_activation_ack_handler(&envelope, payload);
	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ(semantic_activation_ack_local_stage_ahead.count,
				 UINT32_C(0));
	UT_ASSERT(semantic_activation_ack_table_snapshot(table));
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(table->record_generation, UINT64_C(2));
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0x0a));
	UT_ASSERT_EQ(table->source_feature_bitmap, r4);
	UT_ASSERT_EQ(table->target_feature_bitmap, target);
	test_gate_reset();
}

UT_TEST(test_93da2a_next_sample_waits_for_exact_local_open_publish)
{
	const uint32 resource_x_caps
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
		  | PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	const uint64 r4 = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	const uint64 target
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	ClusterSemanticActivationAckTableV1 *table;
	ClusterSemanticActivationAckTableV1 before_duplicate;
	ClusterSemanticActivationAckWireV1 message;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	int peer;
	int node;

	test_gate_reset();
	cluster_node_id = 2;
	/* The durable predecessor is complete, but this member has not yet
	 * published its exact post-OPEN gate. */
	test_gate_publish(2, 0, 2, test_current_epoch, true);
	test_local_capability_word = resource_x_caps;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word = resource_x_caps;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 4; node++) {
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
	}

	table = SemanticActivationAckTable;
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->coordinator_node = 0;
	table->round_nonce = UINT64_C(77);
	table->expected_members_lo = UINT64_C(0x0f);
	table->observed_members_lo = UINT64_C(0x0f);
	table->transition_epoch = test_current_epoch;
	table->record_generation = 3;
	table->source_feature_bitmap = 0;
	table->target_feature_bitmap = r4;
	table->capability_sample_digest = UINT64_C(0x3141592653589793);
	for (node = 0; node < 4; node++) {
		if (node == cluster_node_id)
			UT_ASSERT(semantic_activation_ack_self_tuple(
				node, resource_x_caps, test_current_epoch, 3,
				&table->expected[node]));
		else {
			table->expected[node].node_id = (uint32)node;
			table->expected[node].boot_id
				= test_remote_admitted_incarnations[node];
			table->expected[node].admitted_incarnation
				= test_remote_admitted_incarnations[node];
			table->expected[node].control_connection_generation = 19;
			table->expected[node].capability_word = resource_x_caps;
			table->expected[node].capability_generation = 19;
			table->expected[node].transition_epoch = test_current_epoch;
			table->expected[node].record_generation = 3;
		}
		table->observed[node] = table->expected[node];
	}

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	message.reason = CLUSTER_SEMANTIC_ACTIVATION_OK;
	message.coordinator_node = 0;
	message.transition_epoch = test_current_epoch;
	message.record_generation = 4;
	message.round_nonce = UINT64_C(78);
	message.source_feature_bitmap = r4;
	message.target_feature_bitmap = target;
	message.admitted_members_lo = UINT64_C(0x0f);
	message.capability_word = resource_x_caps;
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.dest_node_id = (uint32)cluster_node_id;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(payload);

	/* Peers may fan out the next SAMPLE before this member receives its
	 * directed REQUEST.  Both frames remain evidence-only while local OPEN
	 * publication is pending. */
	for (peer = 3; peer >= 1; peer -= 2) {
		message.member_node = (uint32)peer;
		message.boot_id = test_remote_admitted_incarnations[peer];
		message.admitted_incarnation = message.boot_id;
		UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
			&message, payload));
		envelope.source_node_id = (uint32)peer;
		cluster_semantic_activation_ack_handler(&envelope, payload);
		semantic_activation_ack_lmon_drain();
	}
	UT_ASSERT_EQ(semantic_activation_ack_local_stage_ahead.count,
				 UINT32_C(2));
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED);

	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.reason = 0;
	message.member_node = (uint32)cluster_node_id;
	message.boot_id = 0;
	message.admitted_incarnation = 0;
	message.capability_word = 0;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	envelope.source_node_id = 0;
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 0);

	/* Exact local OPEN publication makes the retained REQUEST consumable.
	 * It installs once, applies both retained ACKs, and starts one local
	 * positive fan-out. */
	test_gate_publish(4, r4, 3, test_current_epoch, false);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(semantic_activation_ack_local_stage_ahead.count,
				 UINT32_C(0));
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(table->record_generation, UINT64_C(4));
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0x0e));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], node == cluster_node_id ? 0 : 1);

	/* The exact REQUEST replay cannot erase applied peer observations or
	 * start a second fan-out. */
	before_duplicate = *table;
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(memcmp(&before_duplicate, table, sizeof(*table)), 0);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], node == cluster_node_id ? 0 : 1);
	test_gate_reset();
}

UT_TEST(test_93da3_member_retains_barrier_request_and_fans_out_once)
{
	const uint32 resource_x_caps
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
		  | PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	const uint64 r4 = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	const uint64 target
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	ClusterSemanticActivationAckTableV1 *table;
	ClusterSemanticActivationAckTableV1 complete;
	ClusterSemanticActivationAckWireV1 barrier_request;
	ClusterSemanticActivationAckWireV1 message;
	ClusterSemanticActivationReadRequest read_request;
	ClusterSemanticActivationRecord prepare;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	uint8 record_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 digest = 0;
	int node;

	test_gate_reset();
	cluster_node_id = 3;
	test_gate_publish(2, r4, 1, test_current_epoch, false);
	test_local_capability_word = resource_x_caps;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word = resource_x_caps;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 4; node++)
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;

	table = SemanticActivationAckTable;
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	table->coordinator_node = 0;
	table->round_nonce = UINT64_C(79);
	table->expected_members_lo = UINT64_C(0x0f);
	table->observed_members_lo = UINT64_C(0x0b);
	table->transition_epoch = test_current_epoch;
	table->record_generation = 2;
	table->source_feature_bitmap = r4;
	table->target_feature_bitmap = target;
	for (node = 0; node < 4; node++) {
		SemanticActivationAckTuple tuple;

		memset(&tuple, 0, sizeof(tuple));
		if (node == cluster_node_id)
			UT_ASSERT(semantic_activation_ack_self_tuple(
				node, resource_x_caps, test_current_epoch, 2,
				&tuple));
		else {
			tuple.node_id = (uint32)node;
			tuple.boot_id
				= test_remote_admitted_incarnations[node];
			tuple.admitted_incarnation
				= test_remote_admitted_incarnations[node];
			tuple.control_connection_generation = 19;
			tuple.capability_word = resource_x_caps;
			tuple.capability_generation = 19;
			tuple.transition_epoch = test_current_epoch;
			tuple.record_generation = 2;
		}
		if (node != 2)
			table->observed[node] = tuple;
		complete.observed[node] = tuple;
		complete.expected[node] = tuple;
	}
	complete = *table;
	complete.flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
					 | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	complete.observed_members_lo = UINT64_C(0x0f);
	for (node = 0; node < 4; node++) {
		if (node == cluster_node_id)
			UT_ASSERT(semantic_activation_ack_self_tuple(
				node, resource_x_caps, test_current_epoch, 2,
				&complete.observed[node]));
		else {
			complete.observed[node].node_id = (uint32)node;
			complete.observed[node].boot_id
				= test_remote_admitted_incarnations[node];
			complete.observed[node].admitted_incarnation
				= test_remote_admitted_incarnations[node];
			complete.observed[node].control_connection_generation = 19;
			complete.observed[node].capability_word = resource_x_caps;
			complete.observed[node].capability_generation = 19;
			complete.observed[node].transition_epoch = test_current_epoch;
			complete.observed[node].record_generation = 2;
		}
		complete.expected[node] = complete.observed[node];
	}
	UT_ASSERT(semantic_activation_ack_sample_digest(&complete, &digest));

	memset(&barrier_request, 0, sizeof(barrier_request));
	barrier_request.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	barrier_request.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	barrier_request.result
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	barrier_request.coordinator_node = table->coordinator_node;
	barrier_request.member_node = UINT32_C(3);
	barrier_request.transition_epoch = table->transition_epoch;
	barrier_request.record_generation = table->record_generation;
	barrier_request.round_nonce = table->round_nonce;
	barrier_request.source_feature_bitmap = table->source_feature_bitmap;
	barrier_request.target_feature_bitmap = table->target_feature_bitmap;
	barrier_request.rollback_feature_bitmap
		= table->rollback_feature_bitmap;
	barrier_request.admitted_members_lo = table->expected_members_lo;
	barrier_request.admitted_members_hi = table->expected_members_hi;
	barrier_request.capability_sample_digest = digest;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&barrier_request, payload));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = 0;
	envelope.dest_node_id = 3;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();

	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0x0b));
	UT_ASSERT_EQ(test_drain_request_calls, 0);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 0);

	/* The exact duplicate remains a single retained REQUEST and cannot
	 * create authority, invoke a callback, or start a second fan-out. */
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(test_drain_request_calls, 0);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 0);

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	message.reason = CLUSTER_SEMANTIC_ACTIVATION_OK;
	message.coordinator_node = 0;
	message.member_node = 2;
	message.transition_epoch = test_current_epoch;
	message.record_generation = 2;
	message.round_nonce = UINT64_C(79);
	message.source_feature_bitmap = r4;
	message.target_feature_bitmap = target;
	message.admitted_members_lo = UINT64_C(0x0f);
	message.boot_id = test_remote_admitted_incarnations[2];
	message.admitted_incarnation = message.boot_id;
	message.capability_word = resource_x_caps;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	envelope.source_node_id = 2;
	for (node = 0; node < 4; node++)
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();

	UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(table->flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(table->record_generation, UINT64_C(2));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 0);

	/* Installing the retained REQUEST is not the positive BARRIER ACK.
	 * The unchanged member lifecycle must first re-read the durable PREPARE,
	 * close the exact source generation, and prove the Resource-X cutover
	 * digest.  Only that completion may publish the self row and fan out. */
	cluster_semantic_activation_lmon_tick();
	memset(&read_request, 0, sizeof(read_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(
		&read_request));
	/* The member can complete its first read before the current Resource-X
	 * PREPARE image wins the voting majority.  That completed-but-stale
	 * result must be consumed without changing the gate or ACK table, and
	 * must release the local read owner so a later tick can request the
	 * exact current image. */
	memset(&prepare, 0, sizeof(prepare));
	prepare.phase = CLUSTER_SEMANTIC_PHASE_PREPARE;
	prepare.record_generation = 1;
	prepare.transition_epoch = test_current_epoch;
	prepare.coordinator_node = 0;
	prepare.coordinator_incarnation
		= table->expected[0].admitted_incarnation;
	prepare.admitted_members_lo = UINT64_C(0x0f);
	prepare.source_feature_bitmap = r4;
	prepare.target_feature_bitmap = r4;
	prepare.capability_sample_digest = digest;
	UT_ASSERT(cluster_semantic_activation_record_encode(
		&prepare, record_bytes));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		read_request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK,
		false, record_bytes));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u32(
		test_gate_u32(TEST_GATE_CLOSED_OFFSET)), UINT32_C(0));
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 0);
	if (semantic_activation_lmon_record_read_seq != 0) {
		test_gate_reset();
		return;
	}

	cluster_semantic_activation_lmon_tick();
	memset(&read_request, 0, sizeof(read_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(
		&read_request));
	memset(&prepare, 0, sizeof(prepare));
	prepare.phase = CLUSTER_SEMANTIC_PHASE_PREPARE;
	prepare.record_generation = 2;
	prepare.transition_epoch = test_current_epoch;
	prepare.coordinator_node = 0;
	prepare.coordinator_incarnation
		= table->expected[0].admitted_incarnation;
	prepare.admitted_members_lo = UINT64_C(0x0f);
	prepare.source_feature_bitmap = r4;
	prepare.target_feature_bitmap = target;
	prepare.capability_sample_digest = digest;
	UT_ASSERT(cluster_semantic_activation_record_encode(
		&prepare, record_bytes));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		read_request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK,
		false, record_bytes));
	test_resource_x_gate_snapshot_valid = true;
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_FROZEN;
	test_resource_x_gate_snapshot.formation = 17;
	test_resource_x_gate_snapshot.freeze_generation = 1;
	test_resource_x_cutover_digest_valid = true;
	test_resource_x_cutover_token.old_formation = 17;
	test_resource_x_cutover_token.new_formation = 18;
	test_resource_x_cutover_token.freeze_generation = 1;
	test_resource_x_cutover_thawed = false;
	test_resource_x_cutover_digest = UINT64_C(0xa55a9911);
	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ(pg_atomic_read_u32(
		test_gate_u32(TEST_GATE_CLOSED_OFFSET)), UINT32_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), UINT64_C(2));
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0x08));
	for (node = 0; node < 3; node++) {
		ClusterSemanticActivationAckWireV1 sent;

		UT_ASSERT_EQ(test_send_calls[node], 1);
		UT_ASSERT(cluster_semantic_activation_ack_wire_decode(
			test_send_payloads[node], &sent));
		UT_ASSERT_EQ(sent.kind,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK);
		UT_ASSERT_EQ(sent.stage,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
		UT_ASSERT_EQ(sent.member_node, UINT32_C(3));
	}
	UT_ASSERT_EQ(test_send_calls[3], 0);

	/* A duplicate after exact consumption is idempotent: no second local
	 * callback or positive fan-out, and the installed table stays current. */
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&barrier_request, payload));
	envelope.source_node_id = 0;
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0x08));
	UT_ASSERT_EQ(test_drain_request_calls, 0);
	for (node = 0; node < 3; node++)
		UT_ASSERT_EQ(test_send_calls[node], 1);

	/* The retained slot is evidence only.  A conflicting identity, a
	 * formation change, or a gate-generation change clears the whole round
	 * without publishing a local ACK. */
	for (int drift = 0; drift < 3; drift++) {
		test_gate_reset();
		cluster_node_id = 3;
		test_gate_publish(2, r4, 1, test_current_epoch, false);
		test_local_capability_word = resource_x_caps;
		test_peer_capability_word_sample_ok = true;
		test_peer_capability_word = resource_x_caps;
		test_peer_capability_generation = 19;
		test_peer_capability_matches = true;
		for (node = 0; node < 4; node++)
			test_remote_admitted_incarnations[node]
				= UINT64_C(0x100) + (uint64)node;

		table = SemanticActivationAckTable;
		*table = complete;
		pg_atomic_init_u64(&table->publication_seq, 0);
		table->flags = 0;
		table->observed_members_lo = UINT64_C(0x0b);
		table->capability_sample_digest = 0;
		memset(table->expected, 0, sizeof(table->expected));
		memset(&table->observed[2], 0, sizeof(table->observed[2]));

		barrier_request.round_nonce = UINT64_C(79);
		UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
			&barrier_request, payload));
		envelope.source_node_id = 0;
		cluster_semantic_activation_ack_handler(&envelope, payload);
		semantic_activation_ack_lmon_drain();
		UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);

		if (drift == 0) {
			barrier_request.round_nonce++;
			UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
				&barrier_request, payload));
			cluster_semantic_activation_ack_handler(&envelope, payload);
		} else if (drift == 1)
			test_gate_publish(
				4, r4, 1, test_current_epoch + 1, false);
		else
			test_gate_publish(4, r4, 9, test_current_epoch, false);
		semantic_activation_ack_lmon_drain();

		UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
		UT_ASSERT_EQ(table->expected_members_lo, UINT64_C(0));
		UT_ASSERT_EQ(
			semantic_activation_ack_local_pending_send.pending_members_lo,
			UINT64_C(0));
		for (node = 0; node < 4; node++)
			UT_ASSERT_EQ(test_send_calls[node], 0);
	}
	test_gate_reset();
}

UT_TEST(test_93da4_member_direct_barrier_request_waits_for_lifecycle)
{
	const uint32 resource_x_caps
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
		  | PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	const uint64 r4 = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	const uint64 target
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	ClusterSemanticActivationAckTableV1 *table;
	ClusterSemanticActivationAckWireV1 request;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	uint64 digest = 0;
	int node;

	test_gate_reset();
	cluster_node_id = 2;
	test_gate_publish(2, r4, 1, test_current_epoch, false);
	test_local_capability_word = resource_x_caps;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word = resource_x_caps;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 4; node++) {
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
	}

	table = SemanticActivationAckTable;
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->coordinator_node = 0;
	table->round_nonce = UINT64_C(80);
	table->expected_members_lo = UINT64_C(0x0f);
	table->observed_members_lo = UINT64_C(0x0f);
	table->transition_epoch = test_current_epoch;
	table->record_generation = 2;
	table->source_feature_bitmap = r4;
	table->target_feature_bitmap = target;
	for (node = 0; node < 4; node++) {
		SemanticActivationAckTuple tuple;

		memset(&tuple, 0, sizeof(tuple));
		if (node == cluster_node_id)
			UT_ASSERT(semantic_activation_ack_self_tuple(
				node, resource_x_caps, test_current_epoch, 2, &tuple));
		else {
			tuple.node_id = (uint32)node;
			tuple.boot_id = test_remote_admitted_incarnations[node];
			tuple.admitted_incarnation = tuple.boot_id;
			tuple.control_connection_generation = 19;
			tuple.capability_word = resource_x_caps;
			tuple.capability_generation = 19;
			tuple.transition_epoch = test_current_epoch;
			tuple.record_generation = 2;
		}
		table->expected[node] = tuple;
		table->observed[node] = tuple;
	}
	UT_ASSERT(semantic_activation_ack_sample_digest(table, &digest));

	memset(&request, 0, sizeof(request));
	request.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	request.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	request.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	request.coordinator_node = 0;
	request.member_node = (uint32)cluster_node_id;
	request.transition_epoch = test_current_epoch;
	request.record_generation = 2;
	request.round_nonce = table->round_nonce;
	request.source_feature_bitmap = r4;
	request.target_feature_bitmap = target;
	request.admitted_members_lo = table->expected_members_lo;
	request.capability_sample_digest = digest;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&request, payload));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = 0;
	envelope.dest_node_id = (uint32)cluster_node_id;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();

	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0));
	for (node = 0; node < 4; node++) {
		UT_ASSERT_EQ(test_send_calls[node], 0);
	}

	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 0);
	test_gate_reset();
}

UT_TEST(test_g3_ack_complete_matches_round_binding)
{
	/* RF-ROOT P7 G3: the R4 cutover coordinator proof accessor — true only
	 * when the ACK table is COMPLETE (observed == expected) AND bound to
	 * the exact round identity.  The fixture shmem hook points the ACK
	 * table at test_semantic_ack_table.bytes. */
	ClusterSemanticActivationAckTableV1 *table;
	uint64 bit22 = PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;

	test_gate_reset();
	UT_ASSERT(SemanticActivationAckTable
			  == (ClusterSemanticActivationAckTableV1 *)test_semantic_ack_table.bytes);
	table = (ClusterSemanticActivationAckTableV1 *)test_semantic_ack_table.bytes;
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->transition_epoch = 3;
	table->record_generation = 7;
	table->expected_members_lo = UINT64_C(0x0f);
	table->expected_members_hi = 0;
	table->observed_members_lo = UINT64_C(0x0f);
	table->observed_members_hi = 0;
	table->source_feature_bitmap = UINT64_C(1);
	table->target_feature_bitmap = UINT64_C(1) | bit22;
	table->capability_sample_digest = UINT64_C(0xabcd);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;

	/* COMPLETE + round binding at PREPARED satisfies any minimum stage up
	 * to PREPARED (create proof: SAMPLE; activate proof: PREPARED). */
	UT_ASSERT(cluster_semantic_activation_ack_complete_matches(
		3, 7, UINT64_C(0x0f), 0, UINT64_C(1), UINT64_C(1) | bit22,
		UINT64_C(0xabcd), CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE));
	UT_ASSERT(cluster_semantic_activation_ack_complete_matches(
		3, 7, UINT64_C(0x0f), 0, UINT64_C(1), UINT64_C(1) | bit22,
		UINT64_C(0xabcd), CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED));

	/* Stage below the demanded minimum -> false (W6 clause 3: the activate
	 * proof needs the PREPARED-stage all-member ACK, the CLOSED binding). */
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	UT_ASSERT(!cluster_semantic_activation_ack_complete_matches(
		3, 7, UINT64_C(0x0f), 0, UINT64_C(1), UINT64_C(1) | bit22,
		UINT64_C(0xabcd), CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED));
	/* ...but the create proof's SAMPLE minimum still holds at SAMPLE. */
	UT_ASSERT(cluster_semantic_activation_ack_complete_matches(
		3, 7, UINT64_C(0x0f), 0, UINT64_C(1), UINT64_C(1) | bit22,
		UINT64_C(0xabcd), CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE));
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;

	/* Invalid minimum stage -> false (fail-closed). */
	UT_ASSERT(!cluster_semantic_activation_ack_complete_matches(
		3, 7, UINT64_C(0x0f), 0, UINT64_C(1), UINT64_C(1) | bit22,
		UINT64_C(0xabcd), CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_INVALID));

	/* Not COMPLETE -> false. */
	table->flags = 0;
	UT_ASSERT(!cluster_semantic_activation_ack_complete_matches(
		3, 7, UINT64_C(0x0f), 0, UINT64_C(1), UINT64_C(1) | bit22,
		UINT64_C(0xabcd), CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE));
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;

	/* Round binding: wrong epoch / generation / members / digest -> false. */
	UT_ASSERT(!cluster_semantic_activation_ack_complete_matches(
		4, 7, UINT64_C(0x0f), 0, UINT64_C(1), UINT64_C(1) | bit22,
		UINT64_C(0xabcd), CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE));
	UT_ASSERT(!cluster_semantic_activation_ack_complete_matches(
		3, 8, UINT64_C(0x0f), 0, UINT64_C(1), UINT64_C(1) | bit22,
		UINT64_C(0xabcd), CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE));
	UT_ASSERT(!cluster_semantic_activation_ack_complete_matches(
		3, 7, UINT64_C(0x07), 0, UINT64_C(1), UINT64_C(1) | bit22,
		UINT64_C(0xabcd), CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE));
	UT_ASSERT(!cluster_semantic_activation_ack_complete_matches(
		3, 7, UINT64_C(0x0f), 0, UINT64_C(1), UINT64_C(1) | bit22,
		UINT64_C(0xdcba), CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE));

	/* observed != expected -> false (a member has not ACKed). */
	table->observed_members_lo = UINT64_C(0x0e);
	UT_ASSERT(!cluster_semantic_activation_ack_complete_matches(
		3, 7, UINT64_C(0x0f), 0, UINT64_C(1), UINT64_C(1) | bit22,
		UINT64_C(0xabcd), CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE));
	test_gate_reset();
}

UT_TEST(test_93daa_member_accumulates_sample_and_closes_barrier)
{
	ClusterSemanticActivationReadRequest read_request;
	ClusterSemanticActivationRecord prepare;
	ClusterSemanticActivationAckWireV1 message;
	ClusterSemanticActivationAckTableV1 table;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	uint8 record_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 digest = 0;
	int node;

	test_gate_reset();
	cluster_node_id = 3;
	test_gate_publish(2, 0, 7, test_current_epoch, false);
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 3; node++) {
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;
	}

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 0;
	message.member_node = 3;
	message.transition_epoch = test_current_epoch;
	message.record_generation = 8;
	message.round_nonce = UINT64_C(77);
	message.target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	message.admitted_members_lo = UINT64_C(0x0f);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = 0;
	envelope.dest_node_id = 3;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&envelope, payload);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x08));

	for (node = 0; node < 3; node++) {
		message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
		message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
		message.member_node = (uint32)node;
		message.boot_id = test_remote_admitted_incarnations[node];
		message.admitted_incarnation = message.boot_id;
		message.capability_word = test_peer_capability_word;
		UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
			&message, payload));
		envelope.source_node_id = (uint32)node;
		cluster_semantic_activation_ack_handler(&envelope, payload);
	}
	cluster_semantic_activation_lmon_tick();

	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				 | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE);
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x0f));
	for (node = 0; node < 4; node++)
		UT_ASSERT(semantic_activation_ack_matches(
			&table.expected[node], &table.observed[node]));
	UT_ASSERT(semantic_activation_ack_sample_digest(&table, &digest));

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 0;
	message.member_node = 3;
	message.transition_epoch = test_current_epoch;
	message.record_generation = 8;
	message.round_nonce = UINT64_C(77);
	message.target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	message.admitted_members_lo = UINT64_C(0x0f);
	message.capability_sample_digest = digest;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	envelope.source_node_id = 0;
	cluster_semantic_activation_ack_handler(&envelope, payload);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0));

	memset(&read_request, 0, sizeof(read_request));
	if (!cluster_semantic_activation_qvotec_poll_record_read(
			&read_request)) {
		UT_ASSERT(false);
		test_gate_reset();
		return;
	}
	memset(&prepare, 0, sizeof(prepare));
	prepare.phase = CLUSTER_SEMANTIC_PHASE_PREPARE;
	prepare.record_generation = 8;
	prepare.transition_epoch = test_current_epoch;
	prepare.coordinator_node = 0;
	prepare.coordinator_incarnation
		= table.expected[0].admitted_incarnation;
	prepare.admitted_members_lo = UINT64_C(0x0f);
	prepare.target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	prepare.capability_sample_digest = digest;
	UT_ASSERT(cluster_semantic_activation_record_encode(
		&prepare, record_bytes));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		read_request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK,
		false, record_bytes));
	test_reclaim_succeeds = true;
	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), UINT64_C(8));
	UT_ASSERT_EQ(pg_atomic_read_u32(
		test_gate_u32(TEST_GATE_CLOSED_OFFSET)), UINT32_C(1));
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x08));
	UT_ASSERT(semantic_activation_ack_matches(
		&table.expected[3], &table.observed[3]));
	UT_ASSERT_EQ(test_drain_request_calls, 1);
	UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, UINT64_C(0));
	for (node = 0; node < 3; node++) {
		UT_ASSERT_EQ(test_send_calls[node], 2);
		memset(&message, 0, sizeof(message));
		UT_ASSERT(cluster_semantic_activation_ack_wire_decode(
			test_send_payloads[node], &message));
		UT_ASSERT_EQ(message.kind,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK);
		UT_ASSERT_EQ(message.stage,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
		UT_ASSERT_EQ(message.result,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK);
		UT_ASSERT_EQ(message.member_node, UINT32_C(3));
		UT_ASSERT_EQ(message.capability_sample_digest, digest);
	}
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, UINT64_C(0));
	for (node = 0; node < 3; node++)
		UT_ASSERT_EQ(test_send_calls[node], 2);

	for (node = 0; node < 3; node++) {
		memset(&message, 0, sizeof(message));
		message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
		message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
		message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
		message.coordinator_node = 0;
		message.member_node = (uint32)node;
		message.transition_epoch = test_current_epoch;
		message.record_generation = 8;
		message.round_nonce = UINT64_C(77);
		message.target_feature_bitmap
			= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		message.admitted_members_lo = UINT64_C(0x0f);
		message.capability_sample_digest = digest;
		message.boot_id = test_remote_admitted_incarnations[node];
		message.admitted_incarnation = message.boot_id;
		message.capability_word = test_peer_capability_word;
		UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
			&message, payload));
		envelope.source_node_id = (uint32)node;
		cluster_semantic_activation_ack_handler(&envelope, payload);
	}
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				 | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE);

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 0;
	message.member_node = 3;
	message.transition_epoch = test_current_epoch;
	message.record_generation = 8;
	message.round_nonce = UINT64_C(77);
	message.target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	message.admitted_members_lo = UINT64_C(0x0f);
	message.capability_sample_digest = digest;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	envelope.source_node_id = 0;
	cluster_semantic_activation_ack_handler(&envelope, payload);
	cluster_semantic_activation_lmon_tick();

	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(table.observed_members_hi, UINT64_C(0));
	UT_ASSERT_EQ(table.capability_sample_digest, digest);
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), UINT64_C(8));
	UT_ASSERT_EQ(pg_atomic_read_u32(
		test_gate_u32(TEST_GATE_CLOSED_OFFSET)), UINT32_C(1));
	UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, UINT64_C(0));
	for (node = 0; node < 3; node++)
		UT_ASSERT_EQ(test_send_calls[node], 2);

	UT_ASSERT(semantic_activation_ack_lmon_finish_member_prepared(
		&table, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x08));
	UT_ASSERT_EQ(table.observed_members_hi, UINT64_C(0));
	UT_ASSERT(semantic_activation_ack_matches(
		&table.observed[3], &table.expected[3]));
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), UINT64_C(8));
	UT_ASSERT_EQ(pg_atomic_read_u32(
		test_gate_u32(TEST_GATE_CLOSED_OFFSET)), UINT32_C(1));
	UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, UINT64_C(0));
	for (node = 0; node < 3; node++) {
		ClusterSemanticActivationAckWireV1 sent;

		UT_ASSERT_EQ(test_send_calls[node], 3);
		UT_ASSERT(cluster_semantic_activation_ack_wire_decode(
			test_send_payloads[node], &sent));
		UT_ASSERT_EQ(sent.kind,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK);
		UT_ASSERT_EQ(sent.stage,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
		UT_ASSERT_EQ(sent.result,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK);
		UT_ASSERT_EQ(sent.member_node, UINT32_C(3));
	}

	for (node = 0; node < 3; node++) {
		memset(&message, 0, sizeof(message));
		message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
		message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
		message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
		message.coordinator_node = 0;
		message.member_node = (uint32)node;
		message.transition_epoch = test_current_epoch;
		message.record_generation = 8;
		message.round_nonce = UINT64_C(77);
		message.target_feature_bitmap
			= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		message.admitted_members_lo = UINT64_C(0x0f);
		message.capability_sample_digest = digest;
		message.boot_id = test_remote_admitted_incarnations[node];
		message.admitted_incarnation = message.boot_id;
		message.capability_word = test_peer_capability_word;
		UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
			&message, payload));
		envelope.source_node_id = (uint32)node;
		cluster_semantic_activation_ack_handler(&envelope, payload);
	}
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				 | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x0f));

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 0;
	message.member_node = 3;
	message.transition_epoch = test_current_epoch;
	message.record_generation = 9;
	message.round_nonce = UINT64_C(77);
	message.target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	message.admitted_members_lo = UINT64_C(0x0f);
	message.capability_sample_digest = digest;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
		&message, payload));
	envelope.source_node_id = 0;
	cluster_semantic_activation_ack_handler(&envelope, payload);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	if (table.stage
		!= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED) {
		UT_ASSERT_EQ(table.stage,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
		test_gate_reset();
		return;
	}
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(table.record_generation, UINT64_C(9));
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(table.observed_members_hi, UINT64_C(0));
	UT_ASSERT_EQ(table.capability_sample_digest, digest);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(table.expected[node].record_generation,
					 UINT64_C(9));
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), UINT64_C(8));
	UT_ASSERT_EQ(pg_atomic_read_u32(
		test_gate_u32(TEST_GATE_CLOSED_OFFSET)), UINT32_C(1));
	UT_ASSERT(semantic_activation_lmon_record_read_seq != 0);
	for (node = 0; node < 3; node++)
		UT_ASSERT_EQ(test_send_calls[node], 3);

	cluster_semantic_activation_lmon_tick();
	memset(&read_request, 0, sizeof(read_request));
	if (!cluster_semantic_activation_qvotec_poll_record_read(
			&read_request)) {
		UT_ASSERT(false);
		test_gate_reset();
		return;
	}
	memset(&prepare, 0, sizeof(prepare));
	prepare.phase = CLUSTER_SEMANTIC_PHASE_COMMIT;
	prepare.record_generation = 9;
	prepare.transition_epoch = test_current_epoch;
	prepare.coordinator_node = 0;
	prepare.coordinator_incarnation
		= table.expected[0].admitted_incarnation;
	prepare.admitted_members_lo = UINT64_C(0x0f);
	prepare.target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	prepare.capability_sample_digest = digest;
	UT_ASSERT(cluster_semantic_activation_record_encode(
		&prepare, record_bytes));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		read_request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK,
		false, record_bytes));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), UINT64_C(9));
	UT_ASSERT_EQ(pg_atomic_read_u32(
		test_gate_u32(TEST_GATE_CLOSED_OFFSET)), UINT32_C(1));
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, UINT64_C(0));
	for (node = 0; node < 3; node++)
		UT_ASSERT_EQ(test_send_calls[node], 3);

	/* Model only the already-separated successful apply_target_closed return;
	 * the real descriptor remains fail-closed until its owner census exists. */
	UT_ASSERT(semantic_activation_ack_lmon_finish_member_prepared(
		&table, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x08));
	UT_ASSERT(semantic_activation_ack_matches(
		&table.observed[3], &table.expected[3]));
	for (node = 0; node < 3; node++) {
		ClusterSemanticActivationAckWireV1 sent;

		UT_ASSERT_EQ(test_send_calls[node], 4);
		UT_ASSERT(cluster_semantic_activation_ack_wire_decode(
			test_send_payloads[node], &sent));
		UT_ASSERT_EQ(sent.kind,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK);
		UT_ASSERT_EQ(sent.stage,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
		UT_ASSERT_EQ(sent.result,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK);
		UT_ASSERT_EQ(sent.member_node, UINT32_C(3));
		UT_ASSERT_EQ(sent.record_generation, UINT64_C(9));
	}
	test_gate_reset();
}

UT_TEST(test_93db_coordinator_reaches_exact_prepared_origin)
{
	ClusterSemanticActivationCasRequest cas_request;
	ClusterSemanticActivationRecord desired;
	ClusterSemanticActivationAckWireV1 ack;
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationRefusal refusal;
	SemanticActivationUtilityRequest pending_request;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	uint64 digest = 0;
	uint64 prepare_cas_seq = 0;
	uint64 request_seq = 0;
	int node;

	test_gate_reset();
	cluster_node_id = 0;
	test_gate_publish(2, 0, 7, test_current_epoch, false);
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 1; node < 4; node++) {
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;
	}

	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 7,
		&request_seq));
	cluster_semantic_activation_lmon_tick();
	for (node = 1; node < 4; node++) {
		memset(&ack, 0, sizeof(ack));
		ack.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
		ack.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
		ack.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
		ack.coordinator_node = 0;
		ack.member_node = (uint32)node;
		ack.transition_epoch = test_current_epoch;
		ack.record_generation = 8;
		ack.round_nonce = request_seq;
		ack.target_feature_bitmap
			= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		ack.admitted_members_lo = UINT64_C(0x0f);
		ack.boot_id = test_remote_admitted_incarnations[node];
		ack.admitted_incarnation = ack.boot_id;
		ack.capability_word = test_peer_capability_word;
		UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
			&ack, payload));
		memset(&envelope, 0, sizeof(envelope));
		envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
		envelope.source_node_id = (uint32)node;
		envelope.dest_node_id = 0;
		envelope.epoch = test_current_epoch;
		envelope.payload_length = sizeof(payload);
		cluster_semantic_activation_ack_handler(&envelope, payload);
	}

	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				 | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT(semantic_activation_ack_complete_image_current(
		&table, UINT64_C(0x0f), 0, test_current_epoch, 0, 0,
		test_local_capability_word));
	{
		uint64 digest = 0;

		UT_ASSERT(semantic_activation_ack_sample_digest(&table, &digest));
		UT_ASSERT(digest != 0);
	}
	memset(&cas_request, 0, sizeof(cas_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_cas(
		&cas_request));
	UT_ASSERT_EQ(cas_request.expected_generation, UINT64_C(7));
	UT_ASSERT_EQ(cas_request.expected_source_feature_bitmap, UINT64_C(0));
	memset(&desired, 0, sizeof(desired));
	UT_ASSERT(cluster_semantic_activation_record_decode(
		cas_request.desired_bytes, &desired, NULL));
	UT_ASSERT_EQ(desired.phase, CLUSTER_SEMANTIC_PHASE_PREPARE);
	UT_ASSERT_EQ(desired.record_generation, UINT64_C(8));
	UT_ASSERT_EQ(desired.transition_epoch, test_current_epoch);
	UT_ASSERT_EQ(desired.source_feature_bitmap, UINT64_C(0));
	UT_ASSERT_EQ(desired.target_feature_bitmap,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(desired.rollback_feature_bitmap, UINT64_C(0));
	UT_ASSERT_EQ(desired.admitted_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(desired.admitted_members_hi, UINT64_C(0));
	UT_ASSERT(desired.capability_sample_digest != 0);
	UT_ASSERT_EQ(desired.coordinator_node, UINT32_C(0));
	UT_ASSERT_EQ(desired.coordinator_incarnation,
				 test_qvotec_self_incarnation);
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				 | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE);
	UT_ASSERT_EQ(table.capability_sample_digest, UINT64_C(0));
	UT_ASSERT(semantic_activation_ack_sample_digest(&table, &digest));
	UT_ASSERT_EQ(desired.capability_sample_digest, digest);
	memset(&pending_request, 0, sizeof(pending_request));
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending_request));
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		pending_request.request_seq, &refusal));
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), UINT64_C(7));
	UT_ASSERT_EQ(pg_atomic_read_u32(
		test_gate_u32(TEST_GATE_CLOSED_OFFSET)), UINT32_C(0));
	request_seq = cas_request.request_seq;
	prepare_cas_seq = request_seq;
	cluster_semantic_activation_lmon_tick();
	memset(&cas_request, 0, sizeof(cas_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_cas(
		&cas_request));
	UT_ASSERT_EQ(cas_request.request_seq, request_seq);
	UT_ASSERT_EQ(pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq), request_seq);
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_cas(
		request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), UINT64_C(8));
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_ACTIVE_BITS_OFFSET)), UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(
		test_gate_u32(TEST_GATE_CLOSED_OFFSET)), UINT32_C(1));
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	pg_atomic_write_u32(
		test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0), 1);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), UINT64_C(8));
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_ACTIVE_BITS_OFFSET)), UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(
		test_gate_u32(TEST_GATE_CLOSED_OFFSET)), UINT32_C(1));
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(test_drain_request_calls, 0);
	pg_atomic_write_u32(
		test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0), 0);
	test_reclaim_succeeds = true;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(1));
	UT_ASSERT_EQ(table.observed_members_hi, UINT64_C(0));
	UT_ASSERT_EQ(table.capability_sample_digest, digest);
	UT_ASSERT_EQ(test_drain_request_calls, 1);
	for (node = 1; node < 4; node++) {
		memset(&ack, 0, sizeof(ack));
		UT_ASSERT_EQ(test_send_calls[node], 4);
		UT_ASSERT(cluster_semantic_activation_ack_wire_decode(
			test_send_history_payloads[node][2], &ack));
		UT_ASSERT_EQ(ack.kind,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST);
		UT_ASSERT_EQ(ack.stage,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
		UT_ASSERT_EQ(ack.member_node, (uint32)node);
		UT_ASSERT_EQ(ack.record_generation, UINT64_C(8));
		UT_ASSERT_EQ(ack.round_nonce, request_seq);
		UT_ASSERT_EQ(ack.capability_sample_digest, digest);
	}
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	for (node = 1; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 4);

	for (node = 1; node < 4; node++) {
		memset(&ack, 0, sizeof(ack));
		ack.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
		ack.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
		ack.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
		ack.coordinator_node = 0;
		ack.member_node = (uint32)node;
		ack.transition_epoch = test_current_epoch;
		ack.record_generation = 8;
		ack.round_nonce = table.round_nonce;
		ack.target_feature_bitmap
			= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		ack.admitted_members_lo = UINT64_C(0x0f);
		ack.capability_sample_digest = digest;
		ack.boot_id = test_remote_admitted_incarnations[node];
		ack.admitted_incarnation = ack.boot_id;
		ack.capability_word = test_peer_capability_word;
		UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
			&ack, payload));
		memset(&envelope, 0, sizeof(envelope));
		envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
		envelope.source_node_id = (uint32)node;
		envelope.dest_node_id = 0;
		envelope.epoch = test_current_epoch;
		envelope.payload_length = sizeof(payload);
		cluster_semantic_activation_ack_handler(&envelope, payload);
	}
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(table.observed_members_hi, UINT64_C(0));
	UT_ASSERT_EQ(table.capability_sample_digest, digest);
	for (node = 1; node < 4; node++) {
		memset(&ack, 0, sizeof(ack));
		UT_ASSERT_EQ(test_send_calls[node], 5);
		UT_ASSERT(cluster_semantic_activation_ack_wire_decode(
			test_send_payloads[node], &ack));
		UT_ASSERT_EQ(ack.kind,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST);
		UT_ASSERT_EQ(ack.stage,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
		UT_ASSERT_EQ(ack.member_node, (uint32)node);
		UT_ASSERT_EQ(ack.record_generation, UINT64_C(8));
		UT_ASSERT_EQ(ack.round_nonce, table.round_nonce);
		UT_ASSERT_EQ(ack.capability_sample_digest, digest);
	}
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	for (node = 1; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 5);
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending_request));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		pending_request.request_seq, &refusal));

	/* Model only the already-separated successful post-prepare_target return;
	 * the carrier under test begins at the exact PREPARED table. */
	UT_ASSERT(semantic_activation_ack_lmon_finish_member_prepared(
		&table, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x01));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq),
		prepare_cas_seq);

	for (node = 1; node < 4; node++) {
		memset(&ack, 0, sizeof(ack));
		ack.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
		ack.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
		ack.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
		ack.coordinator_node = 0;
		ack.member_node = (uint32)node;
		ack.transition_epoch = test_current_epoch;
		ack.record_generation = 8;
		ack.round_nonce = table.round_nonce;
		ack.target_feature_bitmap
			= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		ack.admitted_members_lo = UINT64_C(0x0f);
		ack.capability_sample_digest = digest;
		ack.boot_id = test_remote_admitted_incarnations[node];
		ack.admitted_incarnation = ack.boot_id;
		ack.capability_word = test_peer_capability_word;
		UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
			&ack, payload));
		memset(&envelope, 0, sizeof(envelope));
		envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
		envelope.source_node_id = (uint32)node;
		envelope.dest_node_id = 0;
		envelope.epoch = test_current_epoch;
		envelope.payload_length = sizeof(payload);
		cluster_semantic_activation_ack_handler(&envelope, payload);
	}
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				 | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE);
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0x0f));
	memset(&cas_request, 0, sizeof(cas_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_cas(
		&cas_request));
	UT_ASSERT_EQ(cas_request.request_seq, prepare_cas_seq + 1);
	UT_ASSERT_EQ(cas_request.expected_generation, UINT64_C(8));
	UT_ASSERT_EQ(cas_request.expected_source_feature_bitmap, UINT64_C(0));
	memset(&desired, 0, sizeof(desired));
	UT_ASSERT(cluster_semantic_activation_record_decode(
		cas_request.desired_bytes, &desired, NULL));
	UT_ASSERT_EQ(desired.phase, CLUSTER_SEMANTIC_PHASE_COMMIT);
	UT_ASSERT_EQ(desired.record_generation, UINT64_C(9));
	UT_ASSERT_EQ(desired.transition_epoch, test_current_epoch);
	UT_ASSERT_EQ(desired.source_feature_bitmap, UINT64_C(0));
	UT_ASSERT_EQ(desired.target_feature_bitmap,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(desired.rollback_feature_bitmap, UINT64_C(0));
	UT_ASSERT_EQ(desired.admitted_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(desired.admitted_members_hi, UINT64_C(0));
	UT_ASSERT_EQ(desired.capability_sample_digest, digest);
	UT_ASSERT_EQ(desired.coordinator_node, UINT32_C(0));
	UT_ASSERT_EQ(desired.coordinator_incarnation,
				 test_qvotec_self_incarnation);
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_cas(
		cas_request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
	cluster_semantic_activation_lmon_tick();
	if (pg_atomic_read_u64(
			test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET))
		!= UINT64_C(9)) {
		UT_ASSERT_EQ(pg_atomic_read_u64(
			test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)),
			UINT64_C(9));
		test_gate_reset();
		return;
	}
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_ACTIVE_BITS_OFFSET)), UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(
		test_gate_u32(TEST_GATE_CLOSED_OFFSET)), UINT32_C(1));
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(table.expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(table.observed_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(table.observed_members_hi, UINT64_C(0));
	UT_ASSERT_EQ(table.record_generation, UINT64_C(9));
	UT_ASSERT_EQ(table.capability_sample_digest, digest);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(table.expected[node].record_generation,
					 UINT64_C(9));
	UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, UINT64_C(0));
	for (node = 1; node < 4; node++) {
		memset(&ack, 0, sizeof(ack));
		UT_ASSERT_EQ(test_send_calls[node], 7);
		UT_ASSERT(cluster_semantic_activation_ack_wire_decode(
			test_send_payloads[node], &ack));
		UT_ASSERT_EQ(ack.kind,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST);
		UT_ASSERT_EQ(ack.stage,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
		UT_ASSERT_EQ(ack.member_node, (uint32)node);
		UT_ASSERT_EQ(ack.record_generation, UINT64_C(9));
		UT_ASSERT_EQ(ack.round_nonce, table.round_nonce);
		UT_ASSERT_EQ(ack.capability_sample_digest, digest);
	}
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending_request));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		pending_request.request_seq, &refusal));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
	for (node = 1; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 7);
	test_gate_reset();
}

UT_TEST(test_93dc_source_removed_round_waits_without_reviving_source)
{
	ClusterSemanticActivationCasRequest cas_request;
	ClusterSemanticActivationRefusal refusal;
	SemanticActivationUtilityRequest pending;
	uint64 writer_generation = 0;
	uint64 request_seq = 0;
	uint64 r4 = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	uint64 r11 = CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;

	test_gate_reset();
	cluster_node_id = 0;
	test_gate_publish(2, r4, 7, test_current_epoch, false);
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
		  | PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word = test_local_capability_word;
	test_peer_capability_generation = 23;
	test_peer_capability_matches = true;
	test_resource_x_gate_snapshot_valid = true;
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_OPEN;
	test_resource_x_gate_snapshot.formation = 0;

	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, r4, r4 | r11, 0, 7,
		&request_seq));
	cluster_semantic_activation_lmon_tick();

	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
	memset(&pending, 0, sizeof(pending));
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending));
	UT_ASSERT_EQ(pending.request_seq, request_seq);
	memset(&cas_request, 0, sizeof(cas_request));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_record_cas(
		&cas_request));
	UT_ASSERT_EQ(cluster_resource_x_writer_path_snapshot(
		&writer_generation), RESOURCE_X_WRITER_CLOSED);
	UT_ASSERT_EQ(writer_generation, UINT64_C(7));
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_ACTIVE_BITS_OFFSET)), r4);
	UT_ASSERT_EQ(pg_atomic_read_u64(
		test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), UINT64_C(7));
	UT_ASSERT_EQ(pg_atomic_read_u32(
		test_gate_u32(TEST_GATE_CLOSED_OFFSET)), UINT32_C(0));
	test_gate_reset();
}

UT_TEST(test_93dca_source_removed_open_target_remains_selectable)
{
	const ClusterSemanticActivationDescriptor *cutover
		= cluster_semantic_activation_r11_resource_x_descriptor();
	uint64 generation = 0;
	uint64 active_bits
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;

	test_gate_reset();
	test_gate_publish(4, active_bits, 10, test_current_epoch, false);
	UT_ASSERT(!cutover->source_available);
	UT_ASSERT_EQ(cluster_resource_x_writer_path_snapshot(&generation),
				 RESOURCE_X_WRITER_TARGET);
	UT_ASSERT_EQ(generation, UINT64_C(10));
	test_gate_reset();
}

UT_TEST(test_93e_utility_wait_returns_only_matching_terminal_result)
{
	SemanticActivationUtilityRequest request;
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;

	test_gate_reset();
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0, &request_seq));
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&request));
	UT_ASSERT(semantic_activation_utility_mailbox_complete(
		request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, 0, 0));
	memset(&refusal, 0xa5, sizeof(refusal));
	UT_ASSERT_EQ(semantic_activation_utility_mailbox_wait(
					 request_seq, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(refusal.feature_bit, UINT64_C(0));
	UT_ASSERT_EQ(refusal.expected_generation, UINT64_C(0));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
}

UT_TEST(test_93ea_utility_wait_does_not_synthesize_elapsed_terminal)
{
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;

	test_gate_reset();
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0, &request_seq));
	test_complete_after_wait_sleeps = 6000;
	test_wait_completion_request_seq = request_seq;
	memset(&refusal, 0xa5, sizeof(refusal));
	UT_ASSERT_EQ(semantic_activation_utility_mailbox_wait(
					 request_seq, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(test_wait_completion_succeeded);
	UT_ASSERT_EQ(test_wait_sleep_calls, 6000);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
}

UT_TEST(test_93f_pgsa_read_mailbox_round_trip_is_qvotec_owned)
{
	ClusterSemanticActivationReadRequest request;
	ClusterSemanticActivationReadCompletion completion;
	ClusterSemanticActivationRecord record;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 request_seq = 0;

	test_gate_reset();
	memset(&record, 0, sizeof(record));
	record.source_feature_bitmap = 0;
	record.target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	record.transition_epoch = test_current_epoch;
	record.record_generation = 1;
	record.admitted_members_lo = UINT64_C(0x0f);
	record.capability_sample_digest = UINT64_C(0x1234);
	record.coordinator_incarnation = UINT64_C(0x55);
	record.coordinator_node = 1;
	record.phase = CLUSTER_SEMANTIC_PHASE_PREPARE;
	UT_ASSERT(cluster_semantic_activation_record_encode(&record, bytes));
	UT_ASSERT(semantic_activation_record_read_mailbox_submit(&request_seq));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(&request));
	UT_ASSERT_EQ(request.request_seq, request_seq);
	UT_ASSERT(!cluster_semantic_activation_qvotec_complete_record_read(
		request_seq + 1, CLUSTER_SEMANTIC_ACTIVATION_OK, false, bytes));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, false, bytes));
	memset(&completion, 0, sizeof(completion));
	UT_ASSERT(semantic_activation_record_read_mailbox_poll_completion(
		request_seq, &completion));
	UT_ASSERT_EQ(completion.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(!completion.implicit_open);
	UT_ASSERT_EQ(memcmp(completion.selected_bytes, bytes, sizeof(bytes)), 0);
}

static bool
test_encode_prepare_record_cas(
	uint64 expected_generation,
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	ClusterSemanticActivationRecord record;

	if (expected_generation == UINT64_MAX || desired == NULL)
		return false;
	memset(&record, 0, sizeof(record));
	record.source_feature_bitmap = 0;
	record.target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	record.transition_epoch = test_current_epoch;
	record.record_generation = expected_generation + 1;
	record.admitted_members_lo = UINT64_C(0x0f);
	record.capability_sample_digest = UINT64_C(0x1234);
	record.coordinator_incarnation = test_qvotec_self_incarnation;
	record.coordinator_node = (uint32)cluster_node_id;
	record.phase = CLUSTER_SEMANTIC_PHASE_PREPARE;
	return cluster_semantic_activation_record_encode(&record, desired);
}

static bool
test_submit_current_prepare_record_cas(
	uint64 *out_request_seq,
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	uint64 utility_request_seq = 0;

	if (out_request_seq == NULL || desired == NULL)
		return false;
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	return semantic_activation_utility_mailbox_submit(
			   CLUSTER_SEMANTIC_ENABLE_ALL, 0,
			   CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
			   &utility_request_seq)
		   && test_encode_prepare_record_cas(0, desired)
		   && semantic_activation_record_cas_mailbox_submit(
			   0, 0, desired, out_request_seq);
}

UT_TEST(test_93fa_wrong_read_completion_cannot_mutate_pending_cas)
{
	ClusterSemanticActivationCasRequest request;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 zero[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };
	uint64 request_seq = 0;

	test_gate_reset();
	UT_ASSERT(test_submit_current_prepare_record_cas(
		&request_seq, desired));
	UT_ASSERT(!cluster_semantic_activation_qvotec_complete_record_read(
		request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, true, zero));
	memset(&request, 0, sizeof(request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT_EQ(memcmp(request.desired_bytes, desired, sizeof(desired)), 0);
}

UT_TEST(test_93fb_out_of_quorum_qvotec_rejects_pending_record_cas)
{
	ClusterSemanticActivationCasRequest request;
	ClusterSemanticActivationCasRequest zero;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_OK;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 request_seq = 0;

	test_gate_reset();
	UT_ASSERT(test_submit_current_prepare_record_cas(
		&request_seq, desired));
	test_qvotec_in_quorum = false;
	memset(&request, 0xa5, sizeof(request));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT_EQ(memcmp(&request, &zero, sizeof(request)), 0);
	UT_ASSERT(semantic_activation_record_cas_mailbox_poll_completion(
		request_seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
}

UT_TEST(test_93fc_epoch_drift_qvotec_rejects_pending_record_cas)
{
	ClusterSemanticActivationCasRequest request;
	ClusterSemanticActivationCasRequest zero;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_OK;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 request_seq = 0;

	test_gate_reset();
	UT_ASSERT(test_submit_current_prepare_record_cas(
		&request_seq, desired));
	test_current_epoch++;
	memset(&request, 0xa5, sizeof(request));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT_EQ(memcmp(&request, &zero, sizeof(request)), 0);
	UT_ASSERT(semantic_activation_record_cas_mailbox_poll_completion(
		request_seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
}

UT_TEST(test_93fd_incarnation_drift_qvotec_rejects_pending_record_cas)
{
	ClusterSemanticActivationCasRequest request;
	ClusterSemanticActivationCasRequest zero;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_OK;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 request_seq = 0;

	test_gate_reset();
	UT_ASSERT(test_submit_current_prepare_record_cas(
		&request_seq, desired));
	test_qvotec_self_incarnation++;
	memset(&request, 0xa5, sizeof(request));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT_EQ(memcmp(&request, &zero, sizeof(request)), 0);
	UT_ASSERT(semantic_activation_record_cas_mailbox_poll_completion(
		request_seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
}

UT_TEST(test_93fe_record_cas_submit_requires_pending_formation)
{
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 request_seq = 0;
	uint64 utility_request_seq = 0;

	test_gate_reset();
	UT_ASSERT(test_encode_prepare_record_cas(0, desired));
	UT_ASSERT(!semantic_activation_record_cas_mailbox_submit(
		0, 0, desired, &request_seq));

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&utility_request_seq));
	UT_ASSERT(test_encode_prepare_record_cas(0, desired));
	UT_ASSERT(!semantic_activation_record_cas_mailbox_submit(
		0, 0, desired, &request_seq));

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&utility_request_seq));
	UT_ASSERT(test_encode_prepare_record_cas(0, desired));
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	UT_ASSERT(semantic_activation_record_cas_mailbox_submit(
		0, 0, desired, &request_seq));
}

UT_TEST(test_93ff_utility_slot_drift_rejects_pending_record_cas)
{
	ClusterSemanticActivationCasRequest request;
	ClusterSemanticActivationCasRequest zero;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_OK;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 request_seq = 0;
	uint64 utility_request_seq;

	test_gate_reset();
	UT_ASSERT(test_submit_current_prepare_record_cas(
		&request_seq, desired));
	utility_request_seq = pg_atomic_read_u64(
		&SemanticActivationUtilityMailbox->utility_request_seq);
	pg_atomic_write_u64(
		&SemanticActivationUtilityMailbox->utility_request_seq,
		utility_request_seq + 1);
	memset(&request, 0xa5, sizeof(request));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT_EQ(memcmp(&request, &zero, sizeof(request)), 0);
	UT_ASSERT(semantic_activation_record_cas_mailbox_poll_completion(
		request_seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
}

UT_TEST(test_93fg_admitted_basis_drift_rejects_pending_record_cas)
{
	ClusterSemanticActivationCasRequest request;
	ClusterSemanticActivationCasRequest zero;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_OK;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 request_seq = 0;

	test_gate_reset();
	UT_ASSERT(test_submit_current_prepare_record_cas(
		&request_seq, desired));
	test_admitted_snapshot_valid = false;
	memset(&request, 0xa5, sizeof(request));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT_EQ(memcmp(&request, &zero, sizeof(request)), 0);
	UT_ASSERT(semantic_activation_record_cas_mailbox_poll_completion(
		request_seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
}

UT_TEST(test_93fh_utility_expected_generation_drift_rejects_pending_record_cas)
{
	ClusterSemanticActivationCasRequest request;
	ClusterSemanticActivationCasRequest zero;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_OK;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 request_seq = 0;

	test_gate_reset();
	UT_ASSERT(test_submit_current_prepare_record_cas(
		&request_seq, desired));
	SemanticActivationUtilityMailbox->utility_expected_record_generation++;
	memset(&request, 0xa5, sizeof(request));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT_EQ(memcmp(&request, &zero, sizeof(request)), 0);
	UT_ASSERT(semantic_activation_record_cas_mailbox_poll_completion(
		request_seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	test_gate_reset();
}

UT_TEST(test_93fi_pgsa_generation_drift_rejects_pending_record_cas)
{
	ClusterSemanticActivationCasRequest request;
	ClusterSemanticActivationCasRequest zero;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_OK;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 request_seq = 0;

	test_gate_reset();
	UT_ASSERT(test_submit_current_prepare_record_cas(
		&request_seq, desired));
	test_gate_publish(2, 0, 1, test_current_epoch, false);
	memset(&request, 0xa5, sizeof(request));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT_EQ(memcmp(&request, &zero, sizeof(request)), 0);
	UT_ASSERT(semantic_activation_record_cas_mailbox_poll_completion(
		request_seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	test_gate_reset();
}

UT_TEST(test_94_rf_deferred_enable_may_drive_only_preopen_pgrd_setup)
{
	UT_ASSERT(semantic_activation_preopen_pgrd_setup_allowed(
		CLUSTER_SEMANTIC_ENABLE_ALL,
		CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED));
	UT_ASSERT(semantic_activation_preopen_pgrd_setup_allowed(
		CLUSTER_SEMANTIC_ENABLE_ALL,
		CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT(!semantic_activation_preopen_pgrd_setup_allowed(
		CLUSTER_SEMANTIC_DISABLE_ALL,
		CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED));
	UT_ASSERT(!semantic_activation_preopen_pgrd_setup_allowed(
		CLUSTER_SEMANTIC_ENABLE_ALL,
		CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD));
}

UT_TEST(test_94a_public_submit_cannot_bypass_busy_lmon_mailbox)
{
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;

	test_gate_reset();
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0, &request_seq));
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(cluster_semantic_activation_submit(
					 CLUSTER_SEMANTIC_ENABLE_ALL, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	UT_ASSERT_EQ(refusal.expected_generation, UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq), UINT64_C(0));
	test_gate_reset();
}

UT_TEST(test_94ab_initial_clean_candidate_can_enter_sample_without_replacement)
{
	const uint64 system_identifier = UINT64_C(0x1020304050607080);
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterSemanticActivationRefusal refusal;
	int node;

	test_gate_reset();
	cluster_node_id = 0;
	test_current_epoch = CLUSTER_EPOCH_INITIAL;
	test_membership_snapshot_epoch = CLUSTER_EPOCH_INITIAL;
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	test_initial_clean_snapshot_valid = true;
	test_initial_clean_snapshot.formation_marker_generation = 0;
	test_initial_clean_snapshot.formation_epoch = test_current_epoch;
	test_initial_clean_snapshot.members_lo = UINT64_C(0x0f);
	test_initial_clean_snapshot.members_hi = 0;
	test_initial_clean_snapshot.arbiter_node = 0;
	test_initial_clean_snapshot.arbiter_incarnation
		= UINT64_C(0x445566778899aabb);
	test_system_identifier = system_identifier;
	cluster_shared_data_dir = "/cluster-share";
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_generation = 19;
	for (node = 0; node < 4; node++)
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x445566778899aabb) + (uint64)node;
	for (node = 0; node < 4; node++)
		test_initial_clean_snapshot.admitted_incarnation[node]
			= test_remote_admitted_incarnations[node];
	test_last_admitted_incarnation
		= test_remote_admitted_incarnations[cluster_node_id];

	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	memset(descriptor.root_uuid, 0x5a, sizeof(descriptor.root_uuid));
	descriptor.namespace_id = 1;
	descriptor.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(
		&descriptor, test_pgrd_candidate));
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
	UT_ASSERT(semantic_activation_pgrd_snapshot_publish(
		test_pgrd_candidate));

	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(r4_pre_prepare_readiness(0, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(test_admitted_snapshot_calls, 1);
	UT_ASSERT(test_initial_clean_snapshot_valid);
	test_gate_reset();
}

UT_TEST(test_94ac_initial_clean_prepare_waits_for_exact_full_sample_ack)
{
	const uint64 system_identifier = UINT64_C(0x8070605040302010);
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationAckWireV1 ack;
	ClusterSemanticActivationCasRequest cas_request;
	ClusterSemanticActivationRecord desired;
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterUndoRootDescriptorRequest pgrd_request;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	uint64 clean_incarnation;
	uint64 request_seq = 0;
	int node;

	test_gate_reset();
	cluster_node_id = 0;
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	test_initial_clean_snapshot_valid = true;
	test_initial_clean_snapshot.formation_marker_generation = 3;
	test_initial_clean_snapshot.formation_epoch = test_current_epoch;
	test_initial_clean_snapshot.members_lo = UINT64_C(0x0f);
	test_initial_clean_snapshot.arbiter_node = 0;
	test_initial_clean_snapshot.arbiter_incarnation
		= test_qvotec_self_incarnation;
	test_system_identifier = system_identifier;
	cluster_shared_data_dir = "/cluster-share";
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 4; node++) {
		test_remote_admitted_incarnations[node]
			= test_qvotec_self_incarnation + (uint64)node;
		test_initial_clean_snapshot.admitted_incarnation[node]
			= test_remote_admitted_incarnations[node];
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
	}
	test_last_admitted_incarnation
		= test_initial_clean_snapshot.admitted_incarnation[0];
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	memset(descriptor.root_uuid, 0x6b, sizeof(descriptor.root_uuid));
	descriptor.namespace_id = 1;
	descriptor.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(
		&descriptor, test_pgrd_candidate));
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
	UT_ASSERT(semantic_activation_pgrd_snapshot_publish(
		test_pgrd_candidate));
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&request_seq));

	cluster_semantic_activation_lmon_tick();
	memset(&pgrd_request, 0, sizeof(pgrd_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
		&pgrd_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
		pgrd_request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	memset(&cas_request, 0, sizeof(cas_request));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_record_cas(
		&cas_request));

	for (node = 1; node < 4; node++) {
		memset(&ack, 0, sizeof(ack));
		ack.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
		ack.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
		ack.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
		ack.coordinator_node = 0;
		ack.member_node = (uint32)node;
		ack.transition_epoch = test_current_epoch;
		ack.record_generation = 1;
		ack.round_nonce = request_seq;
		ack.target_feature_bitmap
			= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		ack.admitted_members_lo = UINT64_C(0x0f);
		ack.boot_id = test_remote_admitted_incarnations[node];
		ack.admitted_incarnation = ack.boot_id;
		ack.capability_word
			= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
		UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
			&ack, payload));
		memset(&envelope, 0, sizeof(envelope));
		envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
		envelope.source_node_id = (uint32)node;
		envelope.dest_node_id = 0;
		envelope.epoch = test_current_epoch;
		envelope.payload_length = sizeof(payload);
		cluster_semantic_activation_ack_handler(&envelope, payload);
	}
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				 | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE);
	memset(&pgrd_request, 0, sizeof(pgrd_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
		&pgrd_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
		pgrd_request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
	cluster_semantic_activation_lmon_tick();
	memset(&cas_request, 0, sizeof(cas_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_cas(
		&cas_request));
	UT_ASSERT_EQ(cas_request.expected_generation, UINT64_C(0));
	memset(&desired, 0, sizeof(desired));
	UT_ASSERT(cluster_semantic_activation_record_decode(
		cas_request.desired_bytes, &desired, NULL));
	UT_ASSERT_EQ(desired.phase, CLUSTER_SEMANTIC_PHASE_PREPARE);
	UT_ASSERT_EQ(desired.record_generation, UINT64_C(1));
	clean_incarnation
		= test_initial_clean_snapshot.admitted_incarnation[2];
	test_initial_clean_snapshot.admitted_incarnation[2]
		= clean_incarnation + 1;
	memset(&cas_request, 0, sizeof(cas_request));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_record_cas(
		&cas_request));
	test_gate_reset();
}

UT_TEST(test_94ad_initial_clean_basis_is_revalidated_for_commit_and_open)
{
	const uint64 system_identifier = UINT64_C(0x8877665544332211);
	const uint64 digest = UINT64_C(0x1029384756abcdef);
	ClusterSemanticActivationAckTableV1 *table;
	ClusterSemanticActivationRecord desired;
	ClusterSemanticFormationBinding formation;
	ClusterUndoRootDescriptorV1 descriptor;
	uint64 request_seq = 0;
	int node;

	test_gate_reset();
	cluster_node_id = 0;
	test_initial_clean_snapshot_valid = true;
	test_initial_clean_snapshot.formation_marker_generation = 3;
	test_initial_clean_snapshot.formation_epoch = test_current_epoch;
	test_initial_clean_snapshot.members_lo = UINT64_C(0x0f);
	test_initial_clean_snapshot.arbiter_node = 0;
	test_initial_clean_snapshot.arbiter_incarnation
		= test_qvotec_self_incarnation;
	test_system_identifier = system_identifier;
	cluster_shared_data_dir = "/cluster-share";
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 4; node++) {
		test_remote_admitted_incarnations[node]
			= test_qvotec_self_incarnation + (uint64)node;
		test_initial_clean_snapshot.admitted_incarnation[node]
			= test_remote_admitted_incarnations[node];
	}
	test_last_admitted_incarnation
		= test_initial_clean_snapshot.admitted_incarnation[0];
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	memset(descriptor.root_uuid, 0x3d, sizeof(descriptor.root_uuid));
	descriptor.namespace_id = 1;
	descriptor.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(
		&descriptor, test_pgrd_candidate));
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
	UT_ASSERT(semantic_activation_pgrd_snapshot_publish(
		test_pgrd_candidate));
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&request_seq));

	table = SemanticActivationAckTable;
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->coordinator_node = 0;
	table->round_nonce = request_seq;
	table->expected_members_lo = UINT64_C(0x0f);
	table->observed_members_lo = UINT64_C(0x0f);
	table->transition_epoch = test_current_epoch;
	table->record_generation = 1;
	table->target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	table->capability_sample_digest = digest;
	for (node = 0; node < 4; node++) {
		if (node == 0) {
			UT_ASSERT(semantic_activation_ack_self_tuple(
				node, test_local_capability_word, test_current_epoch, 1,
				&table->expected[node]));
		} else {
			table->expected[node].node_id = (uint32)node;
			table->expected[node].boot_id
				= test_remote_admitted_incarnations[node];
			table->expected[node].admitted_incarnation
				= test_remote_admitted_incarnations[node];
			table->expected[node].control_connection_generation = 19;
			table->expected[node].capability_word
				= test_peer_capability_word;
			table->expected[node].capability_generation = 19;
			table->expected[node].transition_epoch = test_current_epoch;
			table->expected[node].record_generation = 1;
		}
		table->observed[node] = table->expected[node];
	}
	formation = (ClusterSemanticFormationBinding){
		.utility_request_seq = request_seq,
		.formation_epoch = test_current_epoch,
		.coordinator_incarnation = test_qvotec_self_incarnation,
		.expected_record_generation = 1,
	};
	memset(&desired, 0, sizeof(desired));
	desired.phase = CLUSTER_SEMANTIC_PHASE_COMMIT;
	desired.record_generation = 2;
	desired.transition_epoch = test_current_epoch;
	desired.target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	desired.admitted_members_lo = UINT64_C(0x0f);
	desired.capability_sample_digest = digest;
	desired.coordinator_node = 0;
	desired.coordinator_incarnation = test_qvotec_self_incarnation;
	test_gate_publish(2, 0, 1, test_current_epoch, true);
	UT_ASSERT(semantic_activation_record_cas_formation_matches(
		&formation, &desired));

	table->stage
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
	table->record_generation = 2;
	for (node = 0; node < 4; node++) {
		table->expected[node].record_generation = 2;
		table->observed[node].record_generation = 2;
	}
	formation.expected_record_generation = 2;
	desired.phase = CLUSTER_SEMANTIC_PHASE_OPEN;
	desired.record_generation = 3;
	test_gate_publish(4, 0, 2, test_current_epoch, true);
	UT_ASSERT(semantic_activation_record_cas_formation_matches(
		&formation, &desired));
	test_gate_reset();
}

UT_TEST(test_94ae_initial_clean_stage_callbacks_require_exact_current_basis)
{
	const uint64 system_identifier = UINT64_C(0x1122334455667788);
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();
	ClusterSemanticActivationAckTableV1 *table;
	ClusterUndoRootDescriptorV1 pgrd;
	uint64 original_incarnation;
	int node;

	test_gate_reset();
	test_current_epoch = CLUSTER_EPOCH_INITIAL;
	test_membership_snapshot_epoch = CLUSTER_EPOCH_INITIAL;
	cluster_node_id = 0;
	test_initial_clean_snapshot_valid = true;
	test_initial_clean_snapshot.formation_marker_generation = 0;
	test_initial_clean_snapshot.formation_epoch = test_current_epoch;
	test_initial_clean_snapshot.members_lo = UINT64_C(0x0f);
	test_initial_clean_snapshot.members_hi = 0;
	test_initial_clean_snapshot.arbiter_node = 0;
	test_initial_clean_snapshot.arbiter_incarnation = 0;
	test_system_identifier = system_identifier;
	cluster_shared_data_dir = "/cluster-share";
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 4; node++) {
		test_remote_admitted_incarnations[node]
			= test_qvotec_self_incarnation + (uint64)node;
		test_initial_clean_snapshot.admitted_incarnation[node]
			= test_remote_admitted_incarnations[node];
	}
	test_last_admitted_incarnation
		= test_initial_clean_snapshot.admitted_incarnation[0];

	memset(&pgrd, 0, sizeof(pgrd));
	pgrd.descriptor_incarnation = 1;
	pgrd.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	pgrd.owner_node = -1;
	memset(pgrd.root_uuid, 0x47, sizeof(pgrd.root_uuid));
	pgrd.namespace_id = 1;
	pgrd.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(
		&pgrd, test_pgrd_candidate));
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
	UT_ASSERT(semantic_activation_pgrd_snapshot_publish(
		test_pgrd_candidate));

	table = SemanticActivationAckTable;
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	table->coordinator_node = 0;
	table->round_nonce = UINT64_C(41);
	table->expected_members_lo = UINT64_C(0x0f);
	table->transition_epoch = test_current_epoch;
	table->record_generation = 1;
	table->target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	table->capability_sample_digest = UINT64_C(0x3141592653589793);
	for (node = 0; node < 4; node++) {
		if (node == 0) {
			UT_ASSERT(semantic_activation_ack_self_tuple(
				node, test_local_capability_word, test_current_epoch, 1,
				&table->expected[node]));
		} else {
			table->expected[node].node_id = (uint32)node;
			table->expected[node].boot_id
				= test_remote_admitted_incarnations[node];
			table->expected[node].admitted_incarnation
				= test_remote_admitted_incarnations[node];
			table->expected[node].control_connection_generation = 19;
			table->expected[node].capability_word
				= test_peer_capability_word;
			table->expected[node].capability_generation = 19;
			table->expected[node].transition_epoch = test_current_epoch;
			table->expected[node].record_generation = 1;
		}
	}
	test_gate_publish(2, 0, 1, test_current_epoch, true);
	UT_ASSERT_EQ(descriptor->prepare_target(1),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);

	original_incarnation
		= test_initial_clean_snapshot.admitted_incarnation[2];
	test_initial_clean_snapshot.admitted_incarnation[2]++;
	UT_ASSERT_EQ(descriptor->prepare_target(1),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	test_initial_clean_snapshot.admitted_incarnation[2]
		= original_incarnation;

	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
	table->record_generation = 2;
	for (node = 0; node < 4; node++)
		table->expected[node].record_generation = 2;
	test_gate_publish(4, 0, 2, test_current_epoch, true);
	UT_ASSERT_EQ(descriptor->apply_target_closed(2),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);

	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->record_generation = 3;
	table->observed_members_lo = UINT64_C(0x0f);
	for (node = 0; node < 4; node++) {
		table->expected[node].record_generation = 3;
		table->observed[node] = table->expected[node];
	}
	UT_ASSERT_EQ(descriptor->open_target_admission(3),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);

	table->observed[3].capability_generation++;
	UT_ASSERT_EQ(descriptor->open_target_admission(3),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(descriptor->revert_source_closed(3),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	test_gate_reset();
}

UT_TEST(test_94b_utility_cannot_close_source_before_prepare_commit)
{
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0, &request_seq));
	cluster_semantic_activation_lmon_tick();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)),
				 UINT64_C(2));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq), UINT64_C(0));
}

UT_TEST(test_95_dormant_target_enter_has_no_token)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	memset(&token, 0xa5, sizeof(token));
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_TARGET_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED);
	UT_ASSERT(!token.entered);
}

UT_TEST(test_96_source_token_recheck_and_leave_are_generation_scoped)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(token.entered);
	UT_ASSERT(cluster_semantic_activation_recheck(&token));
	cluster_semantic_activation_leave(&token);
	UT_ASSERT(!token.entered);
}

UT_TEST(test_97_old_epoch_completion_is_inert_and_requires_revalidation)
{
	ClusterSemanticActivationCasRequest request;
	ClusterSemanticActivationRecord desired_record;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 before_active_bits;
	uint64 before_generation;
	bool before_closed;
	uint64 seq = 0;
	uint64 utility_request_seq = 0;

	test_gate_reset();
	test_gate_publish(2, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 7,
				  test_current_epoch, true);
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
			| CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		0, 7,
		&utility_request_seq));
	before_active_bits = pg_atomic_read_u64(
		&SemanticActivationShmem->active_bits);
	before_generation = pg_atomic_read_u64(
		&SemanticActivationShmem->record_generation);
	before_closed = pg_atomic_read_u32(
		&SemanticActivationShmem->transition_closed) != 0;

	memset(&desired_record, 0, sizeof(desired_record));
	desired_record.source_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	desired_record.target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	desired_record.transition_epoch = test_current_epoch;
	desired_record.record_generation = 8;
	desired_record.coordinator_node = (uint32)cluster_node_id;
	desired_record.coordinator_incarnation = test_qvotec_self_incarnation;
	desired_record.phase = CLUSTER_SEMANTIC_PHASE_PREPARE;
	UT_ASSERT(cluster_semantic_activation_record_encode(&desired_record, desired));
	UT_ASSERT(semantic_activation_record_cas_mailbox_submit(
		7, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, desired, &seq));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_cas(
		request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
	test_current_epoch++;
	UT_ASSERT(semantic_activation_record_cas_mailbox_poll_completion(seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->active_bits),
				 before_active_bits);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation),
				 before_generation);
	UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationShmem->transition_closed) != 0,
				 before_closed);
	test_gate_reset();
}

UT_TEST(test_98_admission_token_has_frozen_natural_layout)
{
	ClusterSemanticAdmissionToken token;

	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(sizeof(token), 32);
	UT_ASSERT_EQ((Size)((char *)&token.feature_bit - (char *)&token), 0);
	UT_ASSERT_EQ((Size)((char *)&token.record_generation - (char *)&token), 8);
	UT_ASSERT_EQ((Size)((char *)&token.side - (char *)&token), 24);
	UT_ASSERT_EQ((Size)((char *)&token.entered - (char *)&token), 25);
}

UT_TEST(test_99_shared_gate_layout_and_bootstrap_are_fail_closed)
{
	test_gate_reset();
	UT_ASSERT_EQ(test_shmem_requested_size, TEST_SEMANTIC_GATE_SHMEM_BYTES);
	UT_ASSERT_EQ(test_utility_mailbox_requested_size,
				 TEST_SEMANTIC_UTILITY_MAILBOX_BYTES);
	UT_ASSERT_EQ(test_ack_table_requested_size,
				 TEST_SEMANTIC_ACK_TABLE_BYTES);
	UT_ASSERT_EQ(test_pgrd_snapshot_requested_size,
				 TEST_SEMANTIC_PGRD_SNAPSHOT_BYTES);
	UT_ASSERT_EQ(test_bit22_latch_requested_size,
				 TEST_SEMANTIC_BIT22_LATCH_BYTES);
	UT_ASSERT_EQ(cluster_semantic_activation_shmem_size(),
				 TEST_SEMANTIC_GATE_SHMEM_BYTES + TEST_SEMANTIC_UTILITY_MAILBOX_BYTES
					 + TEST_SEMANTIC_ACK_TABLE_BYTES + TEST_SEMANTIC_PGRD_SNAPSHOT_BYTES
					 + MAXALIGN(TEST_SEMANTIC_BIT22_LATCH_BYTES)
					 + MAXALIGN(TEST_SEMANTIC_BIT22_SEAM_BYTES)
					 + MAXALIGN(TEST_SEMANTIC_BIT22_SOURCE_CLOSE_BYTES)
					 + MAXALIGN(sizeof(pg_atomic_uint32)));
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT(SemanticActivationAckTable
			  == (ClusterSemanticActivationAckTableV1 *)test_semantic_ack_table.bytes);
	UT_ASSERT(semantic_activation_bytes_are_zero(
		test_semantic_ack_table.bytes, sizeof(test_semantic_ack_table.bytes)));
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationAckTable->publication_seq), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_ACTIVE_BITS_OFFSET)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_FORMATION_EPOCH_OFFSET)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
}

UT_TEST(test_99a_existing_ack_table_is_preserved_on_attach)
{
	test_gate_reset();
	pg_atomic_write_u64(&SemanticActivationAckTable->publication_seq, 8);
	SemanticActivationAckTable->stage
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	test_shmem_found = true;
	test_utility_mailbox_found = true;
	test_ack_table_found = true;

	cluster_semantic_activation_shmem_init();
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationAckTable->publication_seq), 8);
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
}

UT_TEST(test_100_source_enter_owns_shared_debt_and_epoch_token)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 11, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(token.entered);
	UT_ASSERT_EQ(test_token_formation_epoch(&token), test_current_epoch);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 1);
	UT_ASSERT_EQ(test_exit_registration_count, 1);
}

UT_TEST(test_100a_modifier_bootstrap_source_requires_ordinary_write_gate)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, true);
	UT_ASSERT_EQ(cluster_semantic_activation_modifier_enter(true, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(token.entered);
	UT_ASSERT(cluster_semantic_activation_modifier_recheck(&token, true));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 1);
	cluster_semantic_activation_leave(&token);
}

UT_TEST(test_100b_modifier_bootstrap_source_refuses_replacement_closed_member)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, true);
	UT_ASSERT_EQ(cluster_semantic_activation_modifier_enter(false, &token),
				 CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT(!token.entered);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
}

UT_TEST(test_101_active_source_refuses_before_debt)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 12, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT);
	UT_ASSERT(!token.entered);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
}

UT_TEST(test_102_inactive_target_refuses_before_debt)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 13, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_TARGET_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED);
	UT_ASSERT(!token.entered);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_TARGET_SIDE, 0)), 0);
}

UT_TEST(test_102a_terminal_census_is_the_only_inactive_target_exception)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 13, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter_r4_terminal_census(&token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(token.entered);
	UT_ASSERT_EQ(token.side, CLUSTER_SEMANTIC_TARGET_SIDE);
	UT_ASSERT_EQ(pg_atomic_read_u32(
				 test_gate_inflight(CLUSTER_SEMANTIC_TARGET_SIDE, 0)), 1);
	UT_ASSERT(cluster_semantic_activation_recheck_r4_terminal_census(&token));
	UT_ASSERT(!cluster_semantic_activation_recheck(&token));

	test_gate_publish(4, 0, 14, test_current_epoch, false);
	UT_ASSERT(!cluster_semantic_activation_recheck_r4_terminal_census(&token));
	cluster_semantic_activation_leave(&token);
	UT_ASSERT_EQ(pg_atomic_read_u32(
				 test_gate_inflight(CLUSTER_SEMANTIC_TARGET_SIDE, 0)), 0);

	test_gate_publish(6, 0, 15, test_current_epoch, true);
	UT_ASSERT_EQ(cluster_semantic_activation_enter_r4_terminal_census(&token),
				 CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT(!token.entered);
	UT_ASSERT_EQ(pg_atomic_read_u32(
				 test_gate_inflight(CLUSTER_SEMANTIC_TARGET_SIDE, 0)), 0);
}

UT_TEST(test_103_epoch_drift_invalidates_recheck_without_losing_debt)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 14, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	test_current_epoch++;
	UT_ASSERT(!cluster_semantic_activation_recheck(&token));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 1);
	cluster_semantic_activation_leave(&token);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
}

UT_TEST(test_104_close_invalidates_recheck_and_leave_balances_once)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 15, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	test_gate_publish(4, 0, 15, test_current_epoch, true);
	UT_ASSERT(!cluster_semantic_activation_recheck(&token));
	cluster_semantic_activation_leave(&token);
	cluster_semantic_activation_leave(&token);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
}

UT_TEST(test_105_pid_change_discards_inherited_local_ledger_only)
{
	ClusterSemanticAdmissionToken parent_token;
	ClusterSemanticAdmissionToken child_token;

	test_gate_reset();
	test_gate_publish(2, 0, 16, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &parent_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	MyProcPid = 202;
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &child_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(test_exit_registration_count, 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 2);
	cluster_semantic_activation_leave(&child_token);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 1);
}

UT_TEST(test_106_exit_hook_drains_both_side_ledgers)
{
	ClusterSemanticAdmissionToken source_token;
	ClusterSemanticAdmissionToken target_token;

	test_gate_reset();
	test_gate_publish(2, 0, 17, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &source_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	test_gate_publish(4, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 18, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_TARGET_SIDE, &target_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_NOT_NULL(test_exit_callback);
	if (test_exit_callback != NULL)
		test_exit_callback(0, test_exit_callback_arg);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_TARGET_SIDE, 0)), 0);
}

UT_TEST(test_107_odd_snapshot_is_bounded_closed_without_debt)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(3, 0, 19, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
}

UT_TEST(test_108_nonregistered_feature_is_closed_without_debt)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 20, test_current_epoch, false);
	UT_ASSERT_EQ(
		cluster_semantic_activation_enter(UINT64_C(1) << 7, CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
		CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 7)), 0);
}

UT_TEST(test_109_lmon_without_validated_majority_remains_closed)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)), 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_FORMATION_EPOCH_OFFSET)),
				 test_current_epoch);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT(!token.entered);
}

UT_TEST(test_109a_lmon_publishes_source_open_only_after_majority_legacy_zero)
{
	ClusterSemanticActivationReadRequest request;
	ClusterSemanticAdmissionToken token;
	uint8 zero[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };

	test_gate_reset();
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(&request));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, true, zero));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_ACTIVE_BITS_OFFSET)),
				 UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u64(
				 test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)),
				 UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_FORMATION_EPOCH_OFFSET)),
				 test_current_epoch);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 0);
	UT_ASSERT_EQ(test_membership_snapshot_calls, 1);
	UT_ASSERT_EQ(cluster_semantic_activation_modifier_enter(true, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	cluster_semantic_activation_leave(&token);
}

UT_TEST(test_109b_lmon_legacy_zero_requires_coherent_admitted_membership)
{
	ClusterSemanticActivationReadRequest request;
	uint8 zero[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };

	test_gate_reset();
	test_membership_snapshot_valid = false;
	cluster_semantic_activation_lmon_tick();
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(&request));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, true, zero));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(test_membership_snapshot_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
}

UT_TEST(test_109c_lmon_legacy_zero_rejects_membership_epoch_drift)
{
	ClusterSemanticActivationReadRequest request;
	uint8 zero[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };

	test_gate_reset();
	test_membership_snapshot_epoch = test_current_epoch + 1;
	cluster_semantic_activation_lmon_tick();
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(&request));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, true, zero));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(test_membership_snapshot_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
}

UT_TEST(test_110_lmon_odd_writer_remains_fail_closed)
{
	test_gate_reset();
	test_gate_publish(3, 0, 0, test_current_epoch, true);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)), 3);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
}

UT_TEST(test_111_formation_change_closes_before_debt_drain)
{
	ClusterSemanticAdmissionToken old_token;
	ClusterSemanticAdmissionToken new_token;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_SOURCE_SIDE, &old_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	test_current_epoch++;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_FORMATION_EPOCH_OFFSET)),
				 test_current_epoch);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 1);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_SOURCE_SIDE, &new_token),
				 CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	if (new_token.entered)
		cluster_semantic_activation_leave(&new_token);
	cluster_semantic_activation_leave(&old_token);
}

UT_TEST(test_111a_close_source_publishes_closed_before_debt_result)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 25, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(
					 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
					 CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(r4_descriptor.close_source_admission(25),
				 CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)),
				 UINT64_C(4));
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_ACTIVE_BITS_OFFSET)),
				 UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u64(
				 test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)),
				 UINT64_C(25));
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_FORMATION_EPOCH_OFFSET)),
				 test_current_epoch);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	cluster_semantic_activation_leave(&token);
	UT_ASSERT_EQ(r4_descriptor.close_source_admission(25),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)),
				 UINT64_C(4));
}

UT_TEST(test_112_enter_samples_second_snapshot_before_epoch)
{
	ClusterSemanticAdmissionToken token;
	ClusterSemanticAdmissionResult result;

	test_gate_reset();
	test_gate_publish(2, 0, 21, test_current_epoch, false);
	test_advance_epoch_on_read_barrier = 4;
	result = cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_SOURCE_SIDE, &token);
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED);
	UT_ASSERT(!token.entered);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
	if (token.entered)
		cluster_semantic_activation_leave(&token);
}

UT_TEST(test_113_recheck_samples_snapshot_before_epoch)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 22, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	test_read_barrier_count = 0;
	test_advance_epoch_on_read_barrier = 2;
	UT_ASSERT(!cluster_semantic_activation_recheck(&token));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 1);
	cluster_semantic_activation_leave(&token);
}

UT_TEST(test_114_peer_open_matcher_stays_closed_until_d13_ack_table)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 23, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_TARGET_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	test_peer_capability_matches = true;
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(
		&token, 7, PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
					   | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1
					   | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1
					   | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1,
		0));
	UT_ASSERT_EQ(test_peer_capability_match_calls, 1);
	UT_ASSERT_EQ(test_peer_capability_match_peer, 7);
	UT_ASSERT_EQ(test_peer_capability_match_caps,
				 PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
					 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1
					 | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1
					 | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1);
	UT_ASSERT_EQ(test_peer_capability_match_generation, 0);
	cluster_semantic_activation_leave(&token);
}

UT_TEST(test_115_peer_open_matcher_rejects_invalid_inputs_before_capability_match)
{
	ClusterSemanticAdmissionToken target_token;
	ClusterSemanticAdmissionToken source_token;
	ClusterSemanticAdmissionToken wrong_feature_token;
	uint32 required_caps = PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
						  | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1
						  | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1
						  | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1;

	test_gate_reset();
	test_gate_publish(2, 0, 24, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_SOURCE_SIDE, &source_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&source_token, 7, required_caps, 0));
	UT_ASSERT_EQ(test_peer_capability_match_calls, 0);
	cluster_semantic_activation_leave(&source_token);

	test_gate_reset();
	test_gate_publish(2, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 24, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_TARGET_SIDE, &target_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	wrong_feature_token = target_token;
	wrong_feature_token.feature_bit = 0;
	test_peer_capability_matches = true;
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(NULL, 7, required_caps, 0));
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&(ClusterSemanticAdmissionToken){0}, 7,
															  required_caps, 0));
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&wrong_feature_token, 7,
															  required_caps, 0));
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&target_token, -1, required_caps, 0));
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&target_token, CLUSTER_MAX_NODES,
															  required_caps, 0));
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&target_token, 7, 0, 0));
	UT_ASSERT_EQ(test_peer_capability_match_calls, 0);
	test_current_epoch++;
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&target_token, 7, required_caps, 0));
	UT_ASSERT_EQ(test_peer_capability_match_calls, 0);
	test_current_epoch--;
	test_peer_capability_matches = false;
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&target_token, 7, required_caps, 0));
	UT_ASSERT_EQ(test_peer_capability_match_calls, 1);
	cluster_semantic_activation_leave(&target_token);
}

/* Break caught: authenticated opcode-18 phase-3 ingress must terminate at
 * formation LMON, not remain forever in the process-local handoff.  This test
 * exercises the real codec/ingress and asserts that one LMON tick consumes the
 * exact item and delegates only to the reconfig observer. */
UT_TEST(test_116_formation_lmon_consumes_phase3_handoff)
{
	ClusterReplacementPhase3HandoffItem ignored;
	ClusterReplacementWireMessage message;
	ClusterICEnvelope envelope;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];

	while (cluster_replacement_phase3_handoff_poll_local(&ignored))
		;
	test_gate_reset();
	test_peer_capability_matches = true;
	memset(&message, 0, sizeof(message));
	message.phase = CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY;
	message.target_node_id = 3;
	message.epoch = test_current_epoch - 1;
	message.request_nonce = UINT64_C(0x1112131415161718);
	message.identity0 = UINT64_C(9001);
	message.identity1 = UINT64_C(9002);
	message.body.phase3.jcmk_generation = UINT64_C(41);
	message.body.phase3.episode_state_generation = UINT32_C(17);
	message.grammar_fingerprint
		= CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT;
	UT_ASSERT(cluster_replacement_wire_encode(&message, bytes));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_GES_REQUEST;
	envelope.source_node_id = (uint32)message.target_node_id;
	envelope.dest_node_id = 1;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(bytes);
	UT_ASSERT_EQ((int)cluster_replacement_wire_phase3_ingress_local(
					 &envelope, bytes, sizeof(bytes), message.target_node_id, 1,
					 test_current_epoch, 9),
				 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_ENQUEUED);

	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ((int)cluster_replacement_phase3_handoff_pending_local(), 0);
	UT_ASSERT_EQ(test_phase3_observe_calls, 1);
	UT_ASSERT_EQ(memcmp(&test_phase3_observed_item.message, &message,
						 sizeof(message)),
				 0);
	UT_ASSERT_EQ(test_phase3_observed_item.authenticated_source_node_id, 3);
	UT_ASSERT_EQ(test_phase3_observed_item.local_receiver_node_id, 1);
	UT_ASSERT_EQ((int)test_phase3_observed_item.control_connection_generation, 9);
	UT_ASSERT_EQ(test_peer_capability_match_calls, 1);
	UT_ASSERT_EQ(test_peer_capability_match_peer, 3);
	UT_ASSERT_EQ(test_peer_capability_match_caps, (uint32)0x00100000U);
	UT_ASSERT_EQ(test_peer_capability_match_generation, (uint32)9);
}

UT_TEST(test_117_formation_lmon_submits_exact_mirror_candidate_to_qvotec)
{
	const uint64 system_identifier = UINT64_C(0x1122334455667788);
	ClusterSemanticFormationBinding formation;
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterUndoRootDescriptorRequest request;
	uint64 request_seq = 0;
	uint64 utility_request_seq = 0;
	int i;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	cluster_node_id = 0;
	test_admitted_snapshot_valid = false;
	test_system_identifier = system_identifier;
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	for (i = 0; i < CLUSTER_UNDO_ROOT_UUID_BYTES; i++)
		descriptor.root_uuid[i] = (uint8)(0x40 + i);
	descriptor.namespace_id = 1;
	descriptor.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(
		&descriptor, test_pgrd_candidate));
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&utility_request_seq));
	formation = (ClusterSemanticFormationBinding){
		.utility_request_seq = utility_request_seq,
		.formation_epoch = test_current_epoch,
		.coordinator_incarnation = test_qvotec_self_incarnation,
		.expected_record_generation = 0,
	};

	UT_ASSERT(semantic_activation_lmon_submit_pgrd_exact_retry(
		"/unused/pg_undo", &formation, system_identifier, &request_seq));
	UT_ASSERT_EQ(request_seq, 1);
	memset(&request, 0, sizeof(request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
		&request));
	UT_ASSERT_EQ(request.system_identifier, system_identifier);
	UT_ASSERT_EQ(memcmp(request.desired_bytes, test_pgrd_candidate,
					  sizeof(test_pgrd_candidate)), 0);

	test_gate_reset();
}

UT_TEST(test_118_formation_lmon_waits_for_pgrd_majority_before_carrier)
{
	const uint64 system_identifier = UINT64_C(0x8877665544332211);
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterUndoRootDescriptorRequest pgrd_request;
	ClusterSemanticActivationRefusal refusal;
	uint64 utility_request_seq = 0;
	bool pgrd_polled;
	int i;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	cluster_node_id = 0;
	test_admitted_snapshot_valid = false;
	test_system_identifier = system_identifier;
	cluster_shared_data_dir = "/cluster-share";
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	for (i = 0; i < CLUSTER_UNDO_ROOT_UUID_BYTES; i++)
		descriptor.root_uuid[i] = (uint8)(0x70 + i);
	descriptor.namespace_id = 1;
	descriptor.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(
		&descriptor, test_pgrd_candidate));
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&utility_request_seq));

	cluster_semantic_activation_lmon_tick();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		utility_request_seq, &refusal));
	memset(&pgrd_request, 0, sizeof(pgrd_request));
	pgrd_polled
		= cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
			&pgrd_request);
	UT_ASSERT(pgrd_polled);
	UT_ASSERT_EQ(test_pgrd_candidate_read_calls, 1);
	UT_ASSERT_STR_EQ(test_pgrd_candidate_root_directory,
				 "/cluster-share/pg_undo");
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)),
				 UINT64_C(2));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)),
				 0);
	if (pgrd_polled) {
		UT_ASSERT_EQ(pgrd_request.system_identifier, system_identifier);
		UT_ASSERT_EQ(memcmp(pgrd_request.desired_bytes, test_pgrd_candidate,
						  sizeof(test_pgrd_candidate)), 0);
		UT_ASSERT(
			cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
				pgrd_request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
	}

	cluster_semantic_activation_lmon_tick();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(
		utility_request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(test_pgrd_candidate_read_calls, 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)),
				 UINT64_C(2));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)),
				 0);
	test_gate_reset();
}

UT_TEST(test_119_formation_lmon_reads_zero_quorum_before_fresh_pgrd)
{
	const uint64 system_identifier = UINT64_C(0x1234432112344321);
	ClusterUndoRootDescriptorReadRequest read_request;
	ClusterUndoRootDescriptorRequest provision_request;
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterSemanticActivationRefusal refusal;
	uint8 zero[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] = { 0 };
	uint64 utility_request_seq = 0;
	bool provision_polled;
	int i;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	cluster_node_id = 0;
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	test_system_identifier = system_identifier;
	cluster_shared_data_dir = "/cluster-share";
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&utility_request_seq));

	cluster_semantic_activation_lmon_tick();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		utility_request_seq, &refusal));
	memset(&read_request, 0, sizeof(read_request));
	UT_ASSERT(
		cluster_semantic_activation_qvotec_poll_undo_root_descriptor_read(
			&read_request));
	UT_ASSERT_EQ(read_request.system_identifier, system_identifier);
	UT_ASSERT_EQ(test_pgrd_candidate_read_calls, 1);
	UT_ASSERT_EQ(test_strong_random_calls, 0);
	UT_ASSERT_EQ(test_pgrd_publish_calls, 0);
	memset(&provision_request, 0, sizeof(provision_request));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
		&provision_request));
	UT_ASSERT(
		cluster_semantic_activation_qvotec_complete_undo_root_descriptor_read(
			read_request.request_seq,
			CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED, zero));

	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(test_strong_random_calls, 1);
	UT_ASSERT_EQ(test_pgrd_publish_calls, 1);
	UT_ASSERT_STR_EQ(test_pgrd_publish_root_directory,
				 "/cluster-share/pg_undo");
	UT_ASSERT_EQ(cluster_undo_root_descriptor_decode(
					 test_pgrd_published, system_identifier, &descriptor),
				 CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID);
	UT_ASSERT_EQ(descriptor.descriptor_incarnation, UINT64_C(1));
	UT_ASSERT_EQ(descriptor.root_kind, CLUSTER_UNDO_ROOT_KIND_SHARED);
	UT_ASSERT_EQ(descriptor.owner_node, -1);
	UT_ASSERT_EQ(descriptor.root_ordinal, UINT32_C(0));
	UT_ASSERT_EQ(descriptor.namespace_id, UINT64_C(1));
	for (i = 0; i < CLUSTER_UNDO_ROOT_UUID_BYTES; i++)
		UT_ASSERT_EQ(descriptor.root_uuid[i], UINT8_C(0xa6));
	memset(&provision_request, 0, sizeof(provision_request));
	provision_polled
		= cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
			&provision_request);
	UT_ASSERT(provision_polled);
	if (provision_polled) {
		UT_ASSERT_EQ(memcmp(provision_request.desired_bytes,
						  test_pgrd_published,
						  sizeof(test_pgrd_published)), 0);
		UT_ASSERT(
			cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
				provision_request.request_seq,
				CLUSTER_SEMANTIC_ACTIVATION_OK));
	}
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		utility_request_seq, &refusal));

	cluster_semantic_activation_lmon_tick();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(
		utility_request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(test_pgrd_candidate_read_calls, 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)),
				 UINT64_C(2));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)),
				 0);
	test_gate_reset();
}

UT_TEST(test_120_authority_without_mirror_holds_before_carrier)
{
	const uint64 system_identifier = UINT64_C(0x5555666677778888);
	ClusterUndoRootDescriptorReadRequest read_request;
	ClusterUndoRootDescriptorRequest provision_request;
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterSemanticActivationRefusal refusal;
	uint8 authority[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint64 utility_request_seq = 0;
	int i;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	cluster_node_id = 0;
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	test_system_identifier = system_identifier;
	cluster_shared_data_dir = "/cluster-share";
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&utility_request_seq));
	cluster_semantic_activation_lmon_tick();
	memset(&read_request, 0, sizeof(read_request));
	UT_ASSERT(
		cluster_semantic_activation_qvotec_poll_undo_root_descriptor_read(
			&read_request));

	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	for (i = 0; i < CLUSTER_UNDO_ROOT_UUID_BYTES; i++)
		descriptor.root_uuid[i] = (uint8)(0x20 + i);
	descriptor.namespace_id = 1;
	descriptor.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, authority));
	UT_ASSERT(
		cluster_semantic_activation_qvotec_complete_undo_root_descriptor_read(
			read_request.request_seq, CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID,
			authority));

	cluster_semantic_activation_lmon_tick();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(
		utility_request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	UT_ASSERT_EQ(test_strong_random_calls, 0);
	UT_ASSERT_EQ(test_pgrd_publish_calls, 0);
	memset(&provision_request, 0, sizeof(provision_request));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
		&provision_request));
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)),
				 UINT64_C(2));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)),
				 0);
	test_gate_reset();
}

UT_TEST(test_121_out_of_quorum_coordinator_cannot_submit_pgrd)
{
	const uint64 system_identifier = UINT64_C(0x66778899aabbccdd);
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterUndoRootDescriptorRequest pgrd_request;
	ClusterSemanticActivationRefusal refusal;
	uint64 utility_request_seq = 0;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	cluster_node_id = 0;
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	test_system_identifier = system_identifier;
	cluster_shared_data_dir = "/cluster-share";
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	memset(descriptor.root_uuid, 0x9a, sizeof(descriptor.root_uuid));
	descriptor.namespace_id = 1;
	descriptor.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(
		&descriptor, test_pgrd_candidate));
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
	test_qvotec_in_quorum = false;
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&utility_request_seq));

	cluster_semantic_activation_lmon_tick();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(
		utility_request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	UT_ASSERT_EQ(test_pgrd_candidate_read_calls, 0);
	memset(&pgrd_request, 0, sizeof(pgrd_request));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
		&pgrd_request));
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)),
				 UINT64_C(2));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)),
				 0);
	test_gate_reset();
}

UT_TEST(test_122_epoch_drift_rejects_pending_pgrd_without_mutation)
{
	const uint64 system_identifier = UINT64_C(0x778899aabbccddee);
	ClusterSemanticFormationBinding formation;
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterUndoRootDescriptorRequest pgrd_request;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_OK;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint64 request_seq = 0;
	uint64 utility_request_seq = 0;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	cluster_node_id = 0;
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	test_system_identifier = system_identifier;
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	memset(descriptor.root_uuid, 0x8b, sizeof(descriptor.root_uuid));
	descriptor.namespace_id = 1;
	descriptor.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, desired));
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&utility_request_seq));
	formation = (ClusterSemanticFormationBinding){
		.utility_request_seq = utility_request_seq,
		.formation_epoch = test_current_epoch,
		.coordinator_incarnation = test_qvotec_self_incarnation,
		.expected_record_generation = 0,
	};
	UT_ASSERT(cluster_semantic_activation_undo_root_descriptor_mailbox_submit(
		&formation, system_identifier, desired, &request_seq));
	test_current_epoch++;

	memset(&pgrd_request, 0, sizeof(pgrd_request));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
		&pgrd_request));
	UT_ASSERT(cluster_semantic_activation_undo_root_descriptor_mailbox_poll_completion(
		request_seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	test_gate_reset();
}

UT_TEST(test_123_incarnation_drift_rejects_pending_pgrd_without_mutation)
{
	const uint64 system_identifier = UINT64_C(0x8899aabbccddeeff);
	ClusterSemanticFormationBinding formation;
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterUndoRootDescriptorRequest pgrd_request;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_OK;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint64 request_seq = 0;
	uint64 utility_request_seq = 0;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	cluster_node_id = 0;
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	test_system_identifier = system_identifier;
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	memset(descriptor.root_uuid, 0x7c, sizeof(descriptor.root_uuid));
	descriptor.namespace_id = 1;
	descriptor.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, desired));
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&utility_request_seq));
	formation = (ClusterSemanticFormationBinding){
		.utility_request_seq = utility_request_seq,
		.formation_epoch = test_current_epoch,
		.coordinator_incarnation = test_qvotec_self_incarnation,
		.expected_record_generation = 0,
	};
	UT_ASSERT(cluster_semantic_activation_undo_root_descriptor_mailbox_submit(
		&formation, system_identifier, desired, &request_seq));
	test_qvotec_self_incarnation++;

	memset(&pgrd_request, 0, sizeof(pgrd_request));
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
		&pgrd_request));
	UT_ASSERT(cluster_semantic_activation_undo_root_descriptor_mailbox_poll_completion(
		request_seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	test_gate_reset();
}

UT_TEST(test_124_cold_bootstrap_zero_historical_floor_accepts_live_pgrd_binding)
{
	const uint64 system_identifier = UINT64_C(0x99aabbccddeeff00);
	ClusterSemanticFormationBinding formation;
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterUndoRootDescriptorRequest pgrd_request;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint64 request_seq = 0;
	uint64 utility_request_seq = 0;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	cluster_node_id = 0;
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	test_system_identifier = system_identifier;
	test_last_admitted_incarnation = 0;
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	memset(descriptor.root_uuid, 0x6d, sizeof(descriptor.root_uuid));
	descriptor.namespace_id = 1;
	descriptor.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, desired));
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&utility_request_seq));
	formation = (ClusterSemanticFormationBinding){
		.utility_request_seq = utility_request_seq,
		.formation_epoch = test_current_epoch,
		.coordinator_incarnation = test_qvotec_self_incarnation,
		.expected_record_generation = 0,
	};
	UT_ASSERT(cluster_semantic_activation_undo_root_descriptor_mailbox_submit(
		&formation, system_identifier, desired, &request_seq));

	memset(&pgrd_request, 0, sizeof(pgrd_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
		&pgrd_request));
	UT_ASSERT_EQ(pgrd_request.request_seq, request_seq);
	UT_ASSERT_EQ(pgrd_request.formation.coordinator_incarnation,
				 test_qvotec_self_incarnation);
	UT_ASSERT_EQ(memcmp(pgrd_request.desired_bytes, desired, sizeof(desired)),
				 0);
	test_gate_reset();
}

UT_TEST(test_125_pgrd_snapshot_requires_majority_mirror_and_current_admission)
{
	const uint64 system_identifier = UINT64_C(0x0123456789abcdef);
	ClusterSemanticAdmissionToken token;
	ClusterUndoBlock0ResolvedRoot resolved;
	ClusterUndoRootDescriptorRequest pgrd_request;
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterSemanticActivationRefusal refusal;
	uint64 utility_request_seq = 0;
	int i;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	cluster_node_id = 0;
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	test_system_identifier = system_identifier;
	cluster_shared_data_dir = "/cluster-share";
	UT_ASSERT_EQ(cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	resolved.root_id = UINT64_MAX;
	UT_ASSERT(!cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
		&token, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, &resolved));
	UT_ASSERT_EQ(resolved.root_id, UINT64_MAX);
	cluster_semantic_activation_leave(&token);
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	for (i = 0; i < CLUSTER_UNDO_ROOT_UUID_BYTES; i++)
		descriptor.root_uuid[i] = (uint8)(0x20 + i);
	descriptor.namespace_id = 1;
	descriptor.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(
		&descriptor, test_pgrd_candidate));
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0,
		&utility_request_seq));

	cluster_semantic_activation_lmon_tick();
	memset(&pgrd_request, 0, sizeof(pgrd_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
		&pgrd_request));

	/* A live target token cannot consume the candidate before QVOTEC and
	 * exact mirror proof complete in the same formation episode. */
	test_gate_publish(4, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 1,
				  test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &token),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	resolved = (ClusterUndoBlock0ResolvedRoot){
		.intent = CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL,
		.root_id = UINT64_MAX,
		.root_generation = UINT64_MAX,
	};
	UT_ASSERT(!cluster_semantic_activation_resolve_shared_undo_root(
		&token, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, &resolved));
	UT_ASSERT_EQ(resolved.root_id, UINT64_MAX);
	cluster_semantic_activation_leave(&token);
	test_gate_publish(6, 0, 0, test_current_epoch, false);

	UT_ASSERT(cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
		pgrd_request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
	cluster_semantic_activation_lmon_tick();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(
		utility_request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(test_pgrd_candidate_read_calls, 2);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	resolved.root_id = UINT64_MAX;
	UT_ASSERT(cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
		&token, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, &resolved));
	UT_ASSERT_EQ(resolved.root_id, UINT64_C(32768));
	UT_ASSERT(!cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
		&token, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 2, 257, &resolved));
	cluster_semantic_activation_leave(&token);

	/* M4 consumes the same immutable PGRD root while ordinary TARGET remains
	 * dormant; the census token is the sole scoped pre-OPEN exception. */
	UT_ASSERT_EQ(cluster_semantic_activation_enter_r4_terminal_census(&token),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	resolved.root_id = UINT64_MAX;
	UT_ASSERT(!cluster_semantic_activation_resolve_shared_undo_root(
		&token, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, &resolved));
	UT_ASSERT(!cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
		&token, CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, 1, 1, &resolved));
	UT_ASSERT_EQ(resolved.root_id, UINT64_MAX);
	UT_ASSERT(!cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
		&token, CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0,
		1, 1, &resolved));
	UT_ASSERT_EQ(resolved.root_id, UINT64_MAX);
	UT_ASSERT(cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
		&token, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, &resolved));
	UT_ASSERT_EQ(resolved.root_id, UINT64_C(32768));
	cluster_semantic_activation_leave(&token);

	test_gate_publish(8, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 1,
				  test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &token),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(!cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
		&token, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, &resolved));
	UT_ASSERT(cluster_semantic_activation_resolve_shared_undo_root(
		&token, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, &resolved));
	UT_ASSERT_EQ(resolved.intent, CLUSTER_UNDO_PATH_RUNTIME_SHARED);
	UT_ASSERT_EQ(resolved.root_id, UINT64_C(32768));
	UT_ASSERT_EQ(resolved.root_generation, UINT64_C(1));

	/* PGSA movement independently fences the old token without mutating the
	 * root snapshot.  A new current token consumes the same PGRD identity. */
	test_gate_publish(10, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 2,
				  test_current_epoch, false);
	resolved.root_id = UINT64_MAX;
	UT_ASSERT(!cluster_semantic_activation_resolve_shared_undo_root(
		&token, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, &resolved));
	UT_ASSERT_EQ(resolved.root_id, UINT64_MAX);
	cluster_semantic_activation_leave(&token);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &token),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(cluster_semantic_activation_resolve_shared_undo_root(
		&token, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, &resolved));
	UT_ASSERT_EQ(resolved.root_id, UINT64_C(32768));
	UT_ASSERT_EQ(resolved.root_generation, UINT64_C(1));
	cluster_semantic_activation_leave(&token);

	/* A formation edge closes and forgets the prior episode's PGRD proof.
	 * Reopening SOURCE alone cannot relabel stale descriptor bytes current. */
	test_current_epoch++;
	cluster_semantic_activation_lmon_tick();
	test_gate_publish(12, 0, 0, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	resolved.root_id = UINT64_MAX;
	UT_ASSERT(!cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
		&token, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, &resolved));
	UT_ASSERT_EQ(resolved.root_id, UINT64_MAX);
	cluster_semantic_activation_leave(&token);
	test_current_epoch--;
	test_gate_reset();
}

/* RF-ROOT P7 (contract §B / follow-up §D-3): the bit22 cutover reader latch.
 * Default-inactive + fail-closed without shmem; one-shot apply; monotonic. */
UT_TEST(test_126_bit22_latch_fail_closed_without_shmem)
{
	test_gate_reset();
	SemanticActivationBit22Latch = NULL;
	SemanticActivationBit22Seam = NULL; /* shmem unattached */
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT(!cluster_r4_bit22_cutover_latch_apply(7, 1));
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	test_gate_reset();
}

UT_TEST(test_127_bit22_latch_defaults_inactive_then_apply_flips_and_records_round)
{
	test_gate_reset();
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT(cluster_r4_bit22_cutover_latch_apply(7, 3));
	UT_ASSERT(cluster_r4_bit22_cutover_active());
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationBit22Latch->transition_epoch), 7);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationBit22Latch->round_generation), 3);
	test_gate_reset();
}

UT_TEST(test_128_bit22_latch_second_apply_rejected_and_round_identity_kept)
{
	test_gate_reset();
	UT_ASSERT(cluster_r4_bit22_cutover_latch_apply(7, 3));
	/* RF-ROOT P9 verification (contract): a DIFFERENT-round apply is rejected
	 * and — critically — must not rewrite the bound identity. */
	UT_ASSERT(!cluster_r4_bit22_cutover_latch_apply(8, 4));
	UT_ASSERT(cluster_r4_bit22_cutover_active());
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationBit22Latch->transition_epoch), 7);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationBit22Latch->round_generation), 3);
	test_gate_reset();
}

UT_TEST(test_128b_bit22_latch_same_round_apply_is_idempotent)
{
	/* RF-ROOT P9 verification (contract): a CAS loser of the SAME round must
	 * read as applied — the member's OPEN_APPLIED publication completed
	 * (winner bound the same round identity), so it proceeds to publish
	 * its observed+ACK instead of stalling the round. */
	test_gate_reset();
	UT_ASSERT(cluster_r4_bit22_cutover_latch_apply(7, 3));
	UT_ASSERT(cluster_r4_bit22_cutover_latch_apply(7, 3));
	UT_ASSERT(cluster_r4_bit22_cutover_active());
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationBit22Latch->transition_epoch), 7);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationBit22Latch->round_generation), 3);
	test_gate_reset();
}

UT_TEST(test_129_bit22_latch_rejects_zero_round_identity)
{
	test_gate_reset();
	/* RF-ROOT P9 verification: a fresh cluster's bit22 round legitimately
	 * runs at formation epoch 0 (no R4 history); only the round generation
	 * must be nonzero. */
	UT_ASSERT(!cluster_r4_bit22_cutover_latch_apply(7, 0));
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT(cluster_r4_bit22_cutover_latch_apply(0, 1));
	UT_ASSERT(cluster_r4_bit22_cutover_active());
	test_gate_reset();
}

UT_TEST(test_130_bit22_latch_apply_refused_while_census_red)
{
	test_gate_reset();
	ut_r4fsm_census_ok = false; /* KNOWN-DEFERRED hw_remaster still linked */
	UT_ASSERT(!cluster_r4_bit22_cutover_latch_apply(7, 1));
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	ut_r4fsm_census_ok = true;
	ut_activate_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ut_activate_calls = 0;
	ut_create_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ut_create_calls = 0;
	UT_ASSERT(cluster_r4_bit22_cutover_latch_apply(7, 1));
	UT_ASSERT(cluster_r4_bit22_cutover_active());
	test_gate_reset();
}

/* RF-ROOT P7 (contract): member-side OPEN_APPLIED apply.  Build a valid
 * 2-node bit22-cutover ACK table (stage OPEN_APPLIED, target bit22,
 * round identity {transition_epoch=7, record_generation=5}) and drive the
 * member progress path. */
static void
ut_open_applied_table_setup(void)
{
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	SemanticActivationAckTuple remote;
	int node;

	memset(table, 0, sizeof(*table));
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
	table->coordinator_node = 0;
	table->round_nonce = 42;
	table->transition_epoch = 7;
	table->record_generation = 5;
	table->expected_members_lo = UINT64_C(0x03);
	table->expected_members_hi = 0;
	table->target_feature_bitmap
		= PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	table->capability_sample_digest = UINT64_C(0xabcd);
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	/* The complete-image check derives each non-local member's tuple from
	 * the remote admitted incarnation + peer capability sample, and the
	 * local member's from self_tuple — mirror that here. */
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		if (!cluster_membership_is_member(node))
			continue;
		if (node == cluster_node_id) {
			(void)semantic_activation_ack_self_tuple(
				node, test_local_capability_word, 7, 5,
				&table->expected[node]);
			continue;
		}
		memset(&remote, 0, sizeof(remote));
		remote.node_id = (uint32)node;
		remote.boot_id = test_remote_admitted_incarnations[node];
		remote.admitted_incarnation = test_remote_admitted_incarnations[node];
		remote.control_connection_generation
			= (uint64)test_peer_capability_generation;
		remote.capability_word = test_peer_capability_word;
		remote.capability_generation
			= (uint64)test_peer_capability_generation;
		remote.transition_epoch = 7;
		remote.record_generation = 5;
		table->expected[node] = remote;
	}
}

static void
ut_open_applied_env_setup(void)
{
	int node;

	test_gate_reset();
	cluster_node_id = 1;
	test_qvotec_in_quorum = true;
	test_membership_snapshot_valid = true;
	test_membership_snapshot_lo = UINT64_C(0x03);
	test_membership_snapshot_hi = 0;
	test_membership_snapshot_epoch = 7;
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_generation = 19;
	for (node = 0; node < CLUSTER_MAX_NODES; node++)
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;
	ut_open_applied_table_setup();
}

UT_TEST(test_131_member_open_applied_applies_latch_and_acks)
{
	ut_open_applied_env_setup();
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT(semantic_activation_ack_lmon_progress_member_open_applied(
		SemanticActivationAckTable));
	UT_ASSERT(cluster_r4_bit22_cutover_active());
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationBit22Latch->transition_epoch), 7);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationBit22Latch->round_generation), 5);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo
				 & UINT64_C(0x02), UINT64_C(0x02));
	/* COMPLETE awaits the coordinator's own observed bit, which the
	 * coordinator-side OPEN_APPLIED advance (contract step ②) sets when all
	 * members ACKed — covered there. */
	test_gate_reset();
}

UT_TEST(test_132_member_open_applied_replay_is_idempotent)
{
	ut_open_applied_env_setup();
	UT_ASSERT(semantic_activation_ack_lmon_progress_member_open_applied(
		SemanticActivationAckTable));
	UT_ASSERT(cluster_r4_bit22_cutover_active());
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo
				 & UINT64_C(0x02), UINT64_C(0x02));
	/* Replay (duplicate REQUEST) — the latch is monotonic, the member just
	 * re-ACKs; the observed set and latch round identity must not move. */
	UT_ASSERT(semantic_activation_ack_lmon_progress_member_open_applied(
		SemanticActivationAckTable));
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationBit22Latch->transition_epoch), 7);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationBit22Latch->round_generation), 5);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo
				 & UINT64_C(0x02), UINT64_C(0x02));
	test_gate_reset();
}

UT_TEST(test_133_member_open_applied_rejects_round_without_bit22)
{
	ut_open_applied_env_setup();
	SemanticActivationAckTable->target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1; /* no bit22 */
	UT_ASSERT(semantic_activation_ack_lmon_progress_member_open_applied(
		SemanticActivationAckTable));
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo
				 & UINT64_C(0x02), UINT64_C(0));
	test_gate_reset();
}

UT_TEST(test_134_member_open_applied_coordinator_does_not_apply)
{
	ut_open_applied_env_setup();
	cluster_node_id = 0; /* the coordinator drives, it does not apply */
	UT_ASSERT(!semantic_activation_ack_lmon_progress_member_open_applied(
		SemanticActivationAckTable));
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	test_gate_reset();
}

UT_TEST(test_135_member_open_applied_fail_closed_when_census_red)
{
	ut_open_applied_env_setup();
	ut_r4fsm_census_ok = false; /* a KNOWN-DEFERRED regression turns RED */
	UT_ASSERT(semantic_activation_ack_lmon_progress_member_open_applied(
		SemanticActivationAckTable));
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo
				 & UINT64_C(0x02), UINT64_C(0)); /* un-observed, fail-closed */
	test_gate_reset();
}

/* RF-ROOT P7 (contract, step ②): coordinator-side OPEN_APPLIED advance.
 * Build a PREPARED-COMPLETE bit22-cutover table (2 nodes, target bit22),
 * stage the seam, and drive semantic_activation_ack_lmon_open_applied_advance. */
static void
ut_open_applied_prepared_table_setup(void)
{
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	int node;

	memset(table, 0, sizeof(*table));
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	table->coordinator_node = 0;
	table->round_nonce = 42;
	table->transition_epoch = 7;
	table->record_generation = 5;
	table->expected_members_lo = UINT64_C(0x03);
	table->expected_members_hi = 0;
	table->observed_members_lo = UINT64_C(0x03); /* all-member ACK */
	table->observed_members_hi = 0;
	table->target_feature_bitmap
		= PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	table->capability_sample_digest = UINT64_C(0xabcd);
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		if (!cluster_membership_is_member(node))
			continue;
		if (node == cluster_node_id) {
			(void)semantic_activation_ack_self_tuple(
				node, test_local_capability_word, 7, 5,
				&table->expected[node]);
			(void)semantic_activation_ack_self_tuple(
				node, test_local_capability_word, 7, 5,
				&table->observed[node]);
			continue;
		}
		/* remote tuple (mirror of ut_open_applied_table_setup) */
		table->expected[node].node_id = (uint32)node;
		table->expected[node].boot_id = test_remote_admitted_incarnations[node];
		table->expected[node].admitted_incarnation
			= test_remote_admitted_incarnations[node];
		table->expected[node].control_connection_generation
			= (uint64)test_peer_capability_generation;
		table->expected[node].capability_word = test_peer_capability_word;
		table->expected[node].capability_generation
			= (uint64)test_peer_capability_generation;
		table->expected[node].transition_epoch = 7;
		table->expected[node].record_generation = 5;
		table->observed[node] = table->expected[node];
	}
}

/* RF-ROOT P9 verification: COMMIT_APPLIED stage table (the bit22 round
 * advances PREPARED -> COMMIT_APPLIED with generation P+1 after the root
 * activation; members verify the ACTIVE root and ACK). */
static void
ut_bit22_commit_applied_table_setup(void)
{
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	int node;

	ut_open_applied_prepared_table_setup();
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
	table->record_generation = 6;
	table->observed_members_lo = 0;
	memset(table->observed, 0, sizeof(table->observed));
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		if (!cluster_membership_is_member(node))
			continue;
		table->expected[node].record_generation = 6;
	}
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
}

static void
ut_open_applied_env_setup_coordinator(void)
{
	int node;

	ut_open_applied_env_setup();
	cluster_node_id = 0; /* the coordinator drives */
	test_membership_snapshot_lo = UINT64_C(0x03);
	test_membership_snapshot_epoch = 7;
	for (node = 0; node < CLUSTER_MAX_NODES; node++)
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;
}

UT_TEST(test_136_coordinator_open_applied_advance_activates_and_publishes)
{
	ClusterControlRootFileToken token;
	uint8 sha[PG_SHA256_DIGEST_LENGTH];
	ClusterControlRootMigrationRoundV1 round;

	ut_open_applied_env_setup_coordinator();
	ut_open_applied_prepared_table_setup();
	memset(&token, 0, sizeof(token));
	token.file_txn_seq = 1;
	memset(sha, 0x11, sizeof(sha));
	memset(&round, 0, sizeof(round));
	round.transition_epoch = 7;
	round.prepare_generation = 5;
	UT_ASSERT(cluster_r4_bit22_cutover_seam_store(&token, sha, &round));
	ut_activate_calls = 0;
	UT_ASSERT(semantic_activation_ack_lmon_bit22_commit_applied_begin(
		SemanticActivationAckTable, UINT64_C(0x03), 0, 7, 0,
		test_local_capability_word));
	UT_ASSERT_EQ(ut_activate_calls, 1);
	/* RF-ROOT P9 verification: the coordinator latch is NOT flipped at
	 * COMMIT_APPLIED — it waits for the durable majority OPEN(P+2)
	 * record.  The stage advances to COMMIT_APPLIED (members verify the
	 * ACTIVE root) with a fresh observed set and generation P+1.
	 * RF-ROOT P9 verification part 3 (cold-formation): the coordinator's own
	 * COMMIT_APPLIED observation is its locally-verified ACTIVE root —
	 * it is marked self-observed (bit 0) so observed can equal expected
	 * and the stage completes. */
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
	UT_ASSERT_EQ(SemanticActivationAckTable->record_generation, 6);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo,
				 UINT64_C(0x01));
	test_gate_reset();
}

UT_TEST(test_137_coordinator_open_applied_advance_waits_for_seam)
{
	ut_open_applied_env_setup_coordinator();
	ut_open_applied_prepared_table_setup();
	/* no seam staged */
	ut_activate_calls = 0;
	UT_ASSERT(semantic_activation_ack_lmon_bit22_commit_applied_begin(
		SemanticActivationAckTable, UINT64_C(0x03), 0, 7, 0,
		test_local_capability_word));
	UT_ASSERT_EQ(ut_activate_calls, 0);
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	test_gate_reset();
}

UT_TEST(test_138_coordinator_open_applied_advance_fail_closed_on_activate_failure)
{
	ClusterControlRootFileToken token;
	uint8 sha[PG_SHA256_DIGEST_LENGTH];
	ClusterControlRootMigrationRoundV1 round;

	ut_open_applied_env_setup_coordinator();
	ut_open_applied_prepared_table_setup();
	memset(&token, 0, sizeof(token));
	token.file_txn_seq = 1;
	memset(sha, 0x11, sizeof(sha));
	memset(&round, 0, sizeof(round));
	round.transition_epoch = 7;
	round.prepare_generation = 5;
	UT_ASSERT(cluster_r4_bit22_cutover_seam_store(&token, sha, &round));
	ut_activate_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	UT_ASSERT(semantic_activation_ack_lmon_bit22_commit_applied_begin(
		SemanticActivationAckTable, UINT64_C(0x03), 0, 7, 0,
		test_local_capability_word));
	UT_ASSERT_EQ(ut_activate_calls, 1);
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	test_gate_reset();
}

UT_TEST(test_145_coordinator_latch_refused_leaves_round_commit_applied)
{
	ClusterControlRootFileToken token;
	uint8 sha[PG_SHA256_DIGEST_LENGTH];
	ClusterControlRootMigrationRoundV1 round;

	/* RF-ROOT P9 verification (contract) + #2 closure: the coordinator's latch
	 * flips at the OPEN_APPLIED publication (after the durable majority
	 * OPEN record), BEFORE its observed bit is published — a refused
	 * latch (census-RED regression) leaves the round at COMMIT_APPLIED
	 * with no observed bit and no REQUEST. */
	ut_open_applied_env_setup_coordinator();
	ut_bit22_commit_applied_table_setup();
	memset(&token, 0, sizeof(token));
	token.file_txn_seq = 1;
	memset(sha, 0x11, sizeof(sha));
	memset(&round, 0, sizeof(round));
	round.transition_epoch = 7;
	round.prepare_generation = 5;
	UT_ASSERT(cluster_r4_bit22_cutover_seam_store(&token, sha, &round));
	ut_r4fsm_census_ok = false; /* latch apply refuses (census RED) */
	UT_ASSERT(semantic_activation_ack_lmon_bit22_open_applied_begin(
		SemanticActivationAckTable, UINT64_C(0x03), 0, 7, 0,
		test_local_capability_word));
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 0);
	test_gate_reset();
}

UT_TEST(test_139_coordinator_open_applied_advance_rejects_mismatched_seam)
{
	ClusterControlRootFileToken token;
	uint8 sha[PG_SHA256_DIGEST_LENGTH];
	ClusterControlRootMigrationRoundV1 round;

	ut_open_applied_env_setup_coordinator();
	ut_open_applied_prepared_table_setup();
	memset(&token, 0, sizeof(token));
	token.file_txn_seq = 1;
	memset(sha, 0x11, sizeof(sha));
	memset(&round, 0, sizeof(round));
	round.transition_epoch = 99; /* != table epoch 7 */
	round.prepare_generation = 5;
	UT_ASSERT(cluster_r4_bit22_cutover_seam_store(&token, sha, &round));
	ut_activate_calls = 0;
	UT_ASSERT(semantic_activation_ack_lmon_bit22_commit_applied_begin(
		SemanticActivationAckTable, UINT64_C(0x03), 0, 7, 0,
		test_local_capability_word));
	UT_ASSERT_EQ(ut_activate_calls, 0);
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	test_gate_reset();
}

/* RF-ROOT P7 (contract): member-side PREPARED for the bit22 cutover round —
 * round-parameterized image check + no-op callback. */
UT_TEST(test_140_member_prepared_bit22_round_parameterized)
{
	SemanticActivationAckTuple self;

	ut_open_applied_env_setup(); /* member cluster_node_id = 1 */
	ut_open_applied_table_setup();
	SemanticActivationAckTable->stage
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	UT_ASSERT(semantic_activation_ack_member_prepared_image_current_bit22(
		SemanticActivationAckTable, &self));
	UT_ASSERT_EQ(self.node_id, 1);
	UT_ASSERT_EQ(bit22_stage_ok(5), CLUSTER_SEMANTIC_ACTIVATION_OK);
	test_gate_reset();
}

static void
ut_bit22_request_ahead_predecessor_setup(uint32 predecessor_stage,
										uint64 predecessor_generation)
{
	const uint32 caps = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	int node;

	test_gate_reset();
	cluster_node_id = 2;
	test_membership_snapshot_lo = UINT64_C(0x0f);
	test_membership_snapshot_hi = 0;
	test_membership_snapshot_epoch = 7;
	test_local_capability_word = caps;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word = caps;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 4; node++) {
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
	}

	test_gate_publish(2, 0, 5, 7, true);
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = predecessor_stage;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	table->coordinator_node = 0;
	table->round_nonce = UINT64_C(42);
	table->expected_members_lo = UINT64_C(0x0f);
	table->transition_epoch = 7;
	table->record_generation = predecessor_generation;
	table->source_feature_bitmap = 0;
	table->target_feature_bitmap
		= PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	table->capability_sample_digest = UINT64_C(0xabcd);
	for (node = 0; node < 4; node++) {
		SemanticActivationAckTuple *expected = &table->expected[node];

		expected->node_id = (uint32)node;
		expected->boot_id
			= node == cluster_node_id
				  ? test_qvotec_self_incarnation
				  : test_remote_admitted_incarnations[node];
		expected->admitted_incarnation = expected->boot_id;
		expected->control_connection_generation
			= node == cluster_node_id
				  ? test_qvotec_self_incarnation
				  : (uint64)test_peer_capability_generation;
		expected->capability_word = caps;
		expected->capability_generation
			= expected->control_connection_generation;
		expected->transition_epoch = 7;
		expected->record_generation = predecessor_generation;
	}
}

/* The production mutation this test catches is dropping a bit22 successor
 * REQUEST when its exact predecessor is installed but this member has not
 * published the predecessor's local positive observation yet. */
UT_TEST(test_140a_member_retains_each_bit22_successor_and_fans_out_once)
{
	static const struct {
		uint32 predecessor_stage;
		uint32 request_stage;
		uint64 predecessor_generation;
		uint64 request_generation;
	} cases[] = {
		{CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER,
		 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED, 5, 5},
		{CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED,
		 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED, 5, 6},
		{CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED,
		 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED, 6, 7},
	};
	ClusterSemanticActivationAckWireV1 message;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	Size case_index;

	for (case_index = 0; case_index < lengthof(cases); case_index++) {
		ClusterSemanticActivationAckTableV1 *table;
		int node;

		ut_bit22_request_ahead_predecessor_setup(
			cases[case_index].predecessor_stage,
			cases[case_index].predecessor_generation);
		table = SemanticActivationAckTable;
		memset(&message, 0, sizeof(message));
		message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
		message.stage = cases[case_index].request_stage;
		message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
		message.coordinator_node = 0;
		message.member_node = 2;
		message.transition_epoch = 7;
		message.record_generation = cases[case_index].request_generation;
		message.round_nonce = UINT64_C(42);
		message.source_feature_bitmap = 0;
		message.target_feature_bitmap
			= PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
		message.admitted_members_lo = UINT64_C(0x0f);
		message.capability_sample_digest = UINT64_C(0xabcd);
		UT_ASSERT(cluster_semantic_activation_ack_wire_encode(
			&message, payload));
		memset(&envelope, 0, sizeof(envelope));
		envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
		envelope.source_node_id = 0;
		envelope.dest_node_id = 2;
		envelope.epoch = 7;
		envelope.payload_length = sizeof(payload);

		cluster_semantic_activation_ack_handler(&envelope, payload);
		semantic_activation_ack_lmon_drain();
		UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
		UT_ASSERT_EQ(table->stage, cases[case_index].predecessor_stage);
		UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0));
		for (node = 0; node < 4; node++)
			UT_ASSERT_EQ(test_send_calls[node], 0);

		/* The exact duplicate remains one evidence-only retained object. */
		cluster_semantic_activation_ack_handler(&envelope, payload);
		semantic_activation_ack_lmon_drain();
		UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
		for (node = 0; node < 4; node++)
			UT_ASSERT_EQ(test_send_calls[node], 0);

		/* Complete only this member's exact predecessor observation.  The
		 * next tick must consume the retained REQUEST through the ordinary
		 * bit22 validator, run the existing member lifecycle, and fan out one
		 * positive ACK. */
		table->observed_members_lo = UINT64_C(0x04);
		table->observed[2] = table->expected[2];
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
		UT_ASSERT_EQ(table->stage, cases[case_index].request_stage);
		UT_ASSERT_EQ(table->record_generation,
					 cases[case_index].request_generation);
		UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0x04));
		for (node = 0; node < 4; node++) {
			ClusterSemanticActivationAckWireV1 sent;

			UT_ASSERT_EQ(test_send_calls[node], node == 2 ? 0 : 1);
			if (node == 2)
				continue;
			UT_ASSERT(cluster_semantic_activation_ack_wire_decode(
				test_send_payloads[node], &sent));
			UT_ASSERT_EQ(sent.kind,
						 CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK);
			UT_ASSERT_EQ(sent.stage, cases[case_index].request_stage);
			UT_ASSERT_EQ(sent.member_node, UINT32_C(2));
		}

		/* A post-consumption replay cannot run the stage callback or fan-out
		 * a second time. */
		cluster_semantic_activation_ack_handler(&envelope, payload);
		semantic_activation_ack_lmon_drain();
		UT_ASSERT_EQ(table->stage, cases[case_index].request_stage);
		for (node = 0; node < 4; node++)
			UT_ASSERT_EQ(test_send_calls[node], node == 2 ? 0 : 1);
		test_gate_reset();
	}
}

UT_TEST(test_140b_bit22_retained_successor_drift_invalidates_without_ack)
{
	ClusterSemanticActivationAckTableV1 *table;
	ClusterSemanticActivationAckWireV1 message;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	int node;

	ut_bit22_request_ahead_predecessor_setup(
		CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER, 5);
	table = SemanticActivationAckTable;
	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 0;
	message.member_node = 2;
	message.transition_epoch = 7;
	message.record_generation = 5;
	message.round_nonce = UINT64_C(42);
	message.target_feature_bitmap
		= PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	message.admitted_members_lo = UINT64_C(0x0f);
	message.capability_sample_digest = UINT64_C(0xabcd);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&message, payload));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = 0;
	envelope.dest_node_id = 2;
	envelope.epoch = 7;
	envelope.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);

	test_membership_snapshot_epoch = 8;
	semantic_activation_ack_lmon_drain();
	UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(table->expected_members_lo, UINT64_C(0));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 0);
	test_gate_reset();
}

UT_TEST(test_140c_bit22_prepared_request_waits_for_local_source_close)
{
	ClusterSemanticActivationAckTableV1 *table;
	ClusterSemanticActivationAckWireV1 message;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	int node;

	ut_bit22_request_ahead_predecessor_setup(
		CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER, 5);
	/* The BARRIER table is current, but this member has not yet executed its
	 * local source-close callback.  Retention is still evidence-only. */
	test_gate_publish(2, 0, 4, 7, false);
	table = SemanticActivationAckTable;
	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 0;
	message.member_node = 2;
	message.transition_epoch = 7;
	message.record_generation = 5;
	message.round_nonce = UINT64_C(42);
	message.target_feature_bitmap
		= PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	message.admitted_members_lo = UINT64_C(0x0f);
	message.capability_sample_digest = UINT64_C(0xabcd);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&message, payload));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = 0;
	envelope.dest_node_id = 2;
	envelope.epoch = 7;
	envelope.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 0);

	test_gate_publish(4, 0, 5, 7, true);
	table->observed_members_lo = UINT64_C(0x04);
	table->observed[2] = table->expected[2];
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], node == 2 ? 0 : 1);
	test_gate_reset();
}

/* The initial R4 round has the same legal BARRIER -> PREPARED ordering as
 * later descriptor-backed rounds.  A coordinator can finish its BARRIER and
 * send PREPARED while this member still exposes the exact pre-close gate
 * generation.  That request remains evidence-only until the local source
 * close publishes generation 1, then installs once and fans out one ACK. */
UT_TEST(test_140d_initial_r4_prepared_request_waits_for_local_source_close)
{
	const uint64 r4 = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	ClusterSemanticActivationAckTableV1 *table;
	ClusterSemanticActivationAckWireV1 message;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	int node;

	ut_bit22_request_ahead_predecessor_setup(
		CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER, 1);
	test_gate_publish(2, 0, 0, 7, false);
	table = SemanticActivationAckTable;
	table->target_feature_bitmap = r4;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->observed_members_lo = table->expected_members_lo;
	for (node = 0; node < 4; node++)
		table->observed[node] = table->expected[node];

	memset(&message, 0, sizeof(message));
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	message.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	message.coordinator_node = 0;
	message.member_node = 2;
	message.transition_epoch = 7;
	message.record_generation = 1;
	message.round_nonce = UINT64_C(42);
	message.target_feature_bitmap = r4;
	message.admitted_members_lo = UINT64_C(0x0f);
	message.capability_sample_digest = UINT64_C(0xabcd);
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&message, payload));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = 0;
	envelope.dest_node_id = 2;
	envelope.epoch = 7;
	envelope.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();

	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 0);

	test_gate_publish(4, 0, 1, 7, true);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	/* Installing PREPARED is authority-neutral.  The actual R4 callback also
	 * requires the initial-clean formation/PGRD image, which is deliberately
	 * outside this request-ahead fixture and is covered by the R4 lifecycle
	 * tests.  Model only its already-established successful return here, then
	 * prove that the retained request starts one exact positive fan-out. */
	UT_ASSERT(semantic_activation_ack_lmon_finish_member_prepared(
		table, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0x04));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], node == 2 ? 0 : 1);
	UT_ASSERT(semantic_activation_ack_lmon_finish_member_prepared(
		table, CLUSTER_SEMANTIC_ACTIVATION_OK));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], node == 2 ? 0 : 1);
	test_gate_reset();
}

/* A clean-formation coordinator publishes the durable PGRD before SAMPLE,
 * but each member still owns its node-local snapshot.  PREPARED must drive
 * the existing QVOTEC read mailbox and remain unobserved until that exact
 * authority image also matches the shared mirror. */
UT_TEST(test_140e_initial_r4_member_fetches_pgrd_before_prepared_ack)
{
	const uint64 system_identifier = UINT64_C(0x1122334455667788);
	ClusterSemanticActivationAckTableV1 *table;
	ClusterUndoRootDescriptorReadRequest read_request;
	ClusterUndoRootDescriptorV1 pgrd;
	uint8 snapshot[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	int node;

	test_gate_reset();
	test_current_epoch = CLUSTER_EPOCH_INITIAL;
	test_membership_snapshot_epoch = CLUSTER_EPOCH_INITIAL;
	cluster_node_id = 2;
	test_initial_clean_snapshot_valid = true;
	test_initial_clean_snapshot.formation_marker_generation = 0;
	test_initial_clean_snapshot.formation_epoch = test_current_epoch;
	test_initial_clean_snapshot.members_lo = UINT64_C(0x0f);
	test_initial_clean_snapshot.members_hi = 0;
	test_initial_clean_snapshot.arbiter_node = 0;
	test_initial_clean_snapshot.arbiter_incarnation = 0;
	test_system_identifier = system_identifier;
	cluster_shared_data_dir = "/cluster-share";
	test_local_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 4; node++) {
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;
		test_initial_clean_snapshot.admitted_incarnation[node]
			= test_remote_admitted_incarnations[node];
	}
	test_initial_clean_snapshot.admitted_incarnation[cluster_node_id]
		= test_qvotec_self_incarnation;
	test_last_admitted_incarnation = test_qvotec_self_incarnation;

	memset(&pgrd, 0, sizeof(pgrd));
	pgrd.descriptor_incarnation = 1;
	pgrd.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	pgrd.owner_node = -1;
	memset(pgrd.root_uuid, 0x5c, sizeof(pgrd.root_uuid));
	pgrd.namespace_id = 1;
	pgrd.system_identifier = system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(
		&pgrd, test_pgrd_candidate));
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;

	table = SemanticActivationAckTable;
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	table->coordinator_node = 0;
	table->round_nonce = UINT64_C(42);
	table->expected_members_lo = UINT64_C(0x0f);
	table->observed_members_lo = UINT64_C(0x01);
	table->transition_epoch = test_current_epoch;
	table->record_generation = 1;
	table->target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	table->capability_sample_digest = UINT64_C(0x3141592653589793);
	for (node = 0; node < 4; node++) {
		if (node == cluster_node_id) {
			UT_ASSERT(semantic_activation_ack_self_tuple(
				node, test_local_capability_word, test_current_epoch, 1,
				&table->expected[node]));
		} else {
			table->expected[node].node_id = (uint32)node;
			table->expected[node].boot_id
				= test_remote_admitted_incarnations[node];
			table->expected[node].admitted_incarnation
				= test_remote_admitted_incarnations[node];
			table->expected[node].control_connection_generation = 19;
			table->expected[node].capability_word
				= test_peer_capability_word;
			table->expected[node].capability_generation = 19;
			table->expected[node].transition_epoch = test_current_epoch;
			table->expected[node].record_generation = 1;
		}
	}
	table->observed[0] = table->expected[0];
	test_gate_publish(2, 0, 1, test_current_epoch, true);

	UT_ASSERT(!semantic_activation_pgrd_snapshot_copy(snapshot));
	UT_ASSERT(semantic_activation_ack_lmon_progress_member_barrier());
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0x01));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 0);

	memset(&read_request, 0, sizeof(read_request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_undo_root_descriptor_read(
		&read_request));
	UT_ASSERT_EQ(read_request.system_identifier, system_identifier);
	UT_ASSERT_EQ(read_request.formation.utility_request_seq,
				 UINT64_C(42));
	UT_ASSERT_EQ(read_request.formation.formation_epoch,
				 test_current_epoch);
	UT_ASSERT_EQ(read_request.formation.coordinator_incarnation,
				 test_qvotec_self_incarnation);
	UT_ASSERT_EQ(read_request.formation.expected_record_generation,
				 UINT64_C(1));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_undo_root_descriptor_read(
		read_request.request_seq, CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID,
		test_pgrd_candidate));

	UT_ASSERT(semantic_activation_ack_lmon_progress_member_barrier());
	UT_ASSERT(semantic_activation_pgrd_snapshot_copy(snapshot));
	UT_ASSERT_EQ(memcmp(snapshot, test_pgrd_candidate,
					 sizeof(snapshot)), 0);
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0x05));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], node == cluster_node_id ? 0 : 1);
	test_gate_reset();
}

UT_TEST(test_141_member_prepared_r4_round_keeps_four_member_shape)
{
	SemanticActivationAckTuple self;

	ut_open_applied_env_setup();
	ut_open_applied_table_setup();
	SemanticActivationAckTable->stage
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	SemanticActivationAckTable->target_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1; /* R4 round: no bit22 */
	/* The R4 four-member shape (expected 0x0f, coordinator 0) is violated by
	 * the 2-node table — the frozen R4 check must still reject it. */
	UT_ASSERT(!semantic_activation_ack_member_prepared_image_current(
		SemanticActivationAckTable, &self));
	test_gate_reset();
}

/* RF-ROOT P7 (contract step ④c): the round driver begin. */
static ClusterControlRootMigrationRoundV1
ut_cutover_round(void)
{
	ClusterControlRootMigrationRoundV1 round;

	memset(&round, 0, sizeof(round));
	round.prepare_generation = 5;
	round.transition_epoch = 7;
	round.source_feature_bitmap = 0;
	round.target_feature_bitmap
		= PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	round.admitted_bitmap_low = UINT64_C(0x03);
	round.capability_sample_digest = UINT64_C(0xabcd);
	return round;
}

UT_TEST(test_142_cutover_begin_stages_seam_and_publishes_prepared)
{
	/* RF-ROOT P9 verification (implementation): begin() stages the source-close
	 * BARRIER only; the migration image build + create_prepared + seam +
	 * PREPARED publication happen in the LMON tick once the all-member
	 * BARRIER is COMPLETE.
	 * RF-ROOT P9 verification part 3 (cold-formation): the fresh-cluster bit22
	 * round mints the FIRST PGSA record — the PREPARE CAS writes
	 * generation 1 over the majority legacy-zero implicit-OPEN record
	 * (expected gen 0 -> desired gen 1), so the round runs at
	 * prepare_generation = 1 (the R4 chain then commits gen 2 and opens
	 * gen 3). */
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round = ut_cutover_round();
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	uint64 cas_seq;
	int node;

	round.prepare_generation = 1;
	ut_open_applied_env_setup_coordinator();
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT(cluster_r4_bit22_cutover_begin(&image, &round));
	/* begin: local source frozen, BARRIER staged, no create yet. */
	UT_ASSERT_EQ(ut_create_calls, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationBit22Seam->valid), 0);
	UT_ASSERT_EQ(table->stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(table->record_generation, 1);
	UT_ASSERT_EQ(table->expected_members_lo, UINT64_C(0x03));
	UT_ASSERT((table->target_feature_bitmap
			   & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0);
	UT_ASSERT(cluster_r4_bit22_source_close_current(7, 1));
	UT_ASSERT_EQ(pg_atomic_read_u32(
					 &SemanticActivationBit22SourceClose->writer_count), 0);

	/* All-member BARRIER ACK -> the tick advances: PREPARE(P) CAS ->
	 * build + create + seam + PREPARED stage + REQUEST.  The B′ redo
	 * submits the PREPARE CAS first (advance #1) and only continues
	 * after the durable majority completion (advance #2) — simulate the
	 * QVOTEC-side completion between the two calls. */
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		if (!cluster_membership_is_member(node))
			continue;
		table->observed[node] = table->expected[node];
	}
	table->observed_members_lo = table->expected_members_lo;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	UT_ASSERT(semantic_activation_ack_lmon_bit22_advance());
	cas_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
	UT_ASSERT_EQ(cas_seq, UINT64_C(1));
	pg_atomic_write_u64(&SemanticActivationShmem->record_cas_completion_seq,
						cas_seq);
	pg_atomic_write_u32(&SemanticActivationShmem->record_cas_result,
						CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(semantic_activation_ack_lmon_bit22_advance());
	UT_ASSERT_EQ(ut_create_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationBit22Seam->valid), 1);
	UT_ASSERT_EQ(SemanticActivationBit22Seam->transition_epoch, 7);
	UT_ASSERT_EQ(table->stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	UT_ASSERT_EQ(table->round_nonce, 1);
	UT_ASSERT_EQ(table->record_generation, 1);
	UT_ASSERT_EQ(table->expected_members_lo, UINT64_C(0x03));
	UT_ASSERT((table->target_feature_bitmap
			   & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) != 0);
	UT_ASSERT(table->expected[0].boot_id != 0);
	UT_ASSERT(table->expected[1].boot_id != 0);
	UT_ASSERT(table->expected[1].capability_word
			  == CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS);
	/* Both the BARRIER REQUEST (begin) and the PREPARED REQUEST (tick
	 * advance) went out through the origin mechanism. */
	UT_ASSERT_EQ(semantic_activation_ack_local_request_origin.unsent_members_lo,
				 UINT64_C(0));
	UT_ASSERT_EQ(test_send_calls[1], 2);
	test_gate_reset();
}

UT_TEST(test_143_cutover_begin_rejects_non_coordinator)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round = ut_cutover_round();

	ut_open_applied_env_setup_coordinator();
	cluster_node_id = 1; /* not the coordinator (0) */
	UT_ASSERT(!cluster_r4_bit22_cutover_begin(&image, &round));
	UT_ASSERT_EQ(ut_create_calls, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationBit22Seam->valid), 0);
	test_gate_reset();
}

UT_TEST(test_144_cutover_begin_fail_closed_on_create_failure)
{
	/* RF-ROOT P9 verification: create_prepared runs in the tick after the
	 * all-member BARRIER COMPLETE; a create failure leaves the round at
	 * BARRIER with no seam and no PREPARED stage.  RF-ROOT P9 verification
	 * closure part 3 (cold-formation): the BARRIER COMPLETE first mints the PREPARE
	 * record (majority legacy-zero -> gen 1) — the round runs at
	 * prepare_generation = 1 and the create failure is only reached after
	 * the PREPARE CAS is durable. */
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round = ut_cutover_round();
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	uint64 cas_seq;
	int node;

	round.prepare_generation = 1;
	ut_open_applied_env_setup_coordinator();
	ut_create_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	ut_create_calls = 0;
	UT_ASSERT(cluster_r4_bit22_cutover_begin(&image, &round));
	UT_ASSERT_EQ(ut_create_calls, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationBit22Seam->valid), 0);
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		if (!cluster_membership_is_member(node))
			continue;
		table->observed[node] = table->expected[node];
	}
	table->observed_members_lo = table->expected_members_lo;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	UT_ASSERT(semantic_activation_ack_lmon_bit22_advance());
	cas_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
	UT_ASSERT_EQ(cas_seq, UINT64_C(1));
	pg_atomic_write_u64(&SemanticActivationShmem->record_cas_completion_seq,
						cas_seq);
	pg_atomic_write_u32(&SemanticActivationShmem->record_cas_result,
						CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(!semantic_activation_ack_lmon_bit22_advance());
	UT_ASSERT_EQ(ut_create_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationBit22Seam->valid), 0);
	UT_ASSERT_EQ(table->stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	test_gate_reset();
}

/* R4 cutover contract (verified implementation): restart/reformation OPEN_PROOF
 * reconstruction.  The ACK table is volatile and zeroed at postmaster
 * start, so a post-bit22 restart re-runs the recovery-only OPEN_APPLIED
 * bootstrap from the durable majority OPEN (validated against the ACTIVE
 * root) and arms OPEN_PROOF locally — no wire frame, no ACK inheritance. */
static void
ut_open_proof_env_setup(uint64 generation)
{
	ClusterSemanticActivationRecord record;

	ut_open_applied_env_setup_coordinator(); /* node 0, epoch 7, members 0x03 */
	/* Model the production restart boundary: shmem initialization leaves the
	 * volatile carrier byte-zero before durable OPEN reconstruction. */
	memset(SemanticActivationAckTable, 0,
		   sizeof(*SemanticActivationAckTable));
	UT_ASSERT(cluster_r4_bit22_cutover_latch_apply(7, generation));
	UT_ASSERT(cluster_r4_bit22_cutover_active());
	memset(&record, 0, sizeof(record));
	record.source_feature_bitmap = 0;
	record.target_feature_bitmap
		= PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1
		  | CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	record.transition_epoch = 7;
	record.record_generation = generation;
	record.admitted_members_lo = UINT64_C(0x03);
	record.capability_sample_digest = UINT64_C(0xabcd);
	record.coordinator_incarnation = test_qvotec_self_incarnation;
	record.coordinator_node = 0;
	record.phase = CLUSTER_SEMANTIC_PHASE_OPEN;
	test_r4fsm_bootstrap_read_result = CLUSTER_SEMANTIC_ACTIVATION_OK;
	test_r4fsm_bootstrap_read_ok = true;
	test_r4fsm_bootstrap_implicit_open = true;
	test_r4fsm_root_validate_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	UT_ASSERT(cluster_semantic_activation_record_encode(
		&record, test_r4fsm_bootstrap_bytes));
}

UT_TEST(test_145a_restore_open_proof_reconstructs_table)
{
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;

	ut_open_proof_env_setup(3);
	UT_ASSERT(cluster_semantic_activation_restore_open_proof_if_active());
	UT_ASSERT((table->flags
			   & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF) != 0);
	UT_ASSERT_EQ(table->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED);
	UT_ASSERT_EQ(table->record_generation, UINT64_C(3));
	UT_ASSERT_EQ(table->transition_epoch, UINT64_C(7));
	UT_ASSERT_EQ(table->expected_members_lo, UINT64_C(0x03));
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0x03));
	UT_ASSERT((table->flags & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE) != 0);
	/* a complete current observed image is promoted to expected */
	UT_ASSERT_EQ(memcmp(table->observed, table->expected,
						sizeof(table->expected)), 0);
	UT_ASSERT(table->expected[0].boot_id == test_qvotec_self_incarnation);
	UT_ASSERT(table->expected[1].boot_id
			  == test_remote_admitted_incarnations[1]);
	UT_ASSERT(table->expected[1].capability_word
			  == CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS);
	test_gate_reset();
}

UT_TEST(test_145b_restore_open_proof_fail_closed_on_missing_open)
{
	ut_open_proof_env_setup(3);
	test_r4fsm_bootstrap_read_ok = false;
	UT_ASSERT(!cluster_semantic_activation_restore_open_proof_if_active());
	UT_ASSERT((SemanticActivationAckTable->flags
			   & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF) == 0);
	test_gate_reset();
}

UT_TEST(test_145c_restore_open_proof_fail_closed_on_root_refusal)
{
	ut_open_proof_env_setup(3);
	test_r4fsm_root_validate_result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	UT_ASSERT(!cluster_semantic_activation_restore_open_proof_if_active());
	UT_ASSERT((SemanticActivationAckTable->flags
			   & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF) == 0);
	test_gate_reset();
}

UT_TEST(test_145d_restore_open_proof_fail_closed_on_membership_drift)
{
	ut_open_proof_env_setup(3);
	test_membership_snapshot_lo = UINT64_C(0x01); /* != OPEN admitted 0x03 */
	UT_ASSERT(!cluster_semantic_activation_restore_open_proof_if_active());
	UT_ASSERT((SemanticActivationAckTable->flags
			   & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF) == 0);
	test_gate_reset();
}

UT_TEST(test_145e_restore_open_proof_is_idempotent)
{
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	uint64 seq_before;

	ut_open_proof_env_setup(3);
	UT_ASSERT(cluster_semantic_activation_restore_open_proof_if_active());
	seq_before = pg_atomic_read_u64(&table->publication_seq);
	UT_ASSERT(cluster_semantic_activation_restore_open_proof_if_active());
	UT_ASSERT_EQ(pg_atomic_read_u64(&table->publication_seq), seq_before);
	test_gate_reset();
}

UT_TEST(test_145f_restore_open_proof_requires_active_latch)
{
	ut_open_proof_env_setup(3);
	pg_atomic_write_u32(&SemanticActivationBit22Latch->active, 0);
	UT_ASSERT(!cluster_r4_bit22_cutover_active());
	UT_ASSERT(!cluster_semantic_activation_restore_open_proof_if_active());
	UT_ASSERT((SemanticActivationAckTable->flags
			   & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF) == 0);
	test_gate_reset();
}

UT_TEST(test_145g_peer_open_matches_consumes_open_proof)
{
	ClusterSemanticAdmissionToken token;

	memset(&token, 0, sizeof(token));
	token.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	token.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	token.record_generation = 3;
	token.formation_epoch = 7;
	token.entered = true;
	test_peer_capability_matches = true;

	/* No OPEN_PROOF yet -> §9 refuses without mutation. */
	ut_open_proof_env_setup(3);
	test_gate_publish(2, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 3, 7, false);
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(
		&token, 1, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 19));
	test_gate_reset();

	/* After the reconstruction the exact-generation TARGET token matches. */
	ut_open_proof_env_setup(3);
	test_peer_capability_matches = true; /* gate_reset cleared it */
	test_gate_publish(2, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 3, 7, false);
	UT_ASSERT(cluster_semantic_activation_restore_open_proof_if_active());
	UT_ASSERT(cluster_semantic_activation_recheck(&token));
	UT_ASSERT(cluster_sf_peer_capability_generation_matches(
		1, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 19));
	UT_ASSERT(cluster_semantic_activation_peer_open_matches(
		&token, 1, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 19));
	/* Generation drift refuses. */
	token.record_generation = 4;
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(
		&token, 1, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 19));
	token.record_generation = 3;
	/* Peer outside the member bitmap refuses. */
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(
		&token, 7, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 19));
	test_gate_reset();
}

static void
ut_resource_x_open_carrier_setup_at_epoch(
	ClusterSemanticAdmissionToken *token, uint64 transition_epoch)
{
	const uint32 resource_x_caps
		= CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
		  | PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	const uint64 target_bits
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	int node;

	UT_ASSERT(token != NULL);
	test_capability_missing_peer = -1;
	test_capability_store_missing = false;
	test_gate_reset();
	cluster_node_id = 0;
	test_membership_snapshot_valid = true;
	test_membership_snapshot_lo = UINT64_C(0x0f);
	test_membership_snapshot_hi = 0;
	test_membership_snapshot_epoch = transition_epoch;
	test_current_epoch = transition_epoch;
	test_local_capability_word = resource_x_caps;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word = resource_x_caps;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 4; node++)
		test_remote_admitted_incarnations[node]
			= UINT64_C(0x100) + (uint64)node;

	memset(table, 0, sizeof(*table));
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				 | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->coordinator_node = 0;
	table->round_nonce = 3;
	table->expected_members_lo = UINT64_C(0x0f);
	table->observed_members_lo = UINT64_C(0x0f);
	table->transition_epoch = transition_epoch;
	table->record_generation = 6;
	table->source_feature_bitmap
		= CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	table->target_feature_bitmap = target_bits;
	table->capability_sample_digest = UINT64_C(0xabc123);
	for (node = 0; node < 4; node++) {
		SemanticActivationAckTuple tuple;

		memset(&tuple, 0, sizeof(tuple));
		if (node == cluster_node_id)
			UT_ASSERT(semantic_activation_ack_self_tuple(
				node, test_local_capability_word, transition_epoch, 6,
				&tuple));
		else {
			tuple.node_id = (uint32)node;
			tuple.boot_id = test_remote_admitted_incarnations[node];
			tuple.admitted_incarnation
				= test_remote_admitted_incarnations[node];
			tuple.control_connection_generation = 19;
			tuple.capability_word = test_peer_capability_word;
			tuple.capability_generation = 19;
			tuple.transition_epoch = transition_epoch;
			tuple.record_generation = 6;
		}
		table->expected[node] = tuple;
		table->observed[node] = tuple;
	}

	test_gate_publish(2, target_bits, 6, transition_epoch, false);
	memset(token, 0, sizeof(*token));
	token->feature_bit
		= CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	token->record_generation = 6;
	token->formation_epoch = transition_epoch;
	token->side = CLUSTER_SEMANTIC_TARGET_SIDE;
	token->entered = true;
	UT_ASSERT(cluster_semantic_activation_resource_x_peer_open_matches(
		token, 2, 19));
}

static void
ut_resource_x_open_carrier_setup(ClusterSemanticAdmissionToken *token)
{
	ut_resource_x_open_carrier_setup_at_epoch(token, 7);
}

/* The ordinary cutover retains R4 in its current target bitmap.  Its exact
 * complete OPEN_APPLIED image is not a recovery OPEN_PROOF image: a read-only
 * R4 consumer must accept the same current formation without rewriting it. */
UT_TEST(test_r4_peer_accepts_completed_ordinary_cutover_without_recovery_flag)
{
	ClusterSemanticAdmissionToken token;
	ClusterSemanticActivationAckTableV1 before;
	int epoch;

	for (epoch = 0; epoch <= 7; epoch += 7) {
		ut_resource_x_open_carrier_setup_at_epoch(&token, (uint64)epoch);
		token.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		memcpy(&before, SemanticActivationAckTable, sizeof(before));
		UT_ASSERT(cluster_semantic_activation_recheck(&token));
		UT_ASSERT(semantic_activation_ack_complete_image_current(
			SemanticActivationAckTable, UINT64_C(0x0f), 0, (uint64)epoch, 0, cluster_node_id,
			test_local_capability_word));
		UT_ASSERT(cluster_semantic_activation_peer_open_matches(
			&token, 2, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 19));
		UT_ASSERT_EQ(memcmp(&before, SemanticActivationAckTable, sizeof(before)), 0);
	}
	test_gate_reset();
}

#include "cluster_r4_open_route_test_stubs.h"

UT_TEST(test_ordinary_open_reaches_real_master_route_and_stale_peer_does_not)
{
	ClusterSemanticAdmissionToken token;
	ClusterR4CrRequestPayload request = { 0 };
	ClusterICEnvelope env = { 0 };
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ClusterSemanticActivationAckTableV1 before;

	ut_resource_x_open_carrier_setup_at_epoch(&token, 7);
	before = *SemanticActivationAckTable;
	test_open_route_forwards = 0;
	test_open_route_refusals = 0;
	request.base.request_id = 42;
	request.base.epoch = 7;
	request.base.tag.spcOid = 1663;
	request.base.tag.dbOid = 5;
	request.base.tag.relNumber = 16429;
	request.base.sender_node = 2;
	request.base.requester_backend_id = 7;
	request.base.transition_id = PCM_TRANS_N_TO_S;
	UT_ASSERT(ClusterR4RequestExtensionSetCr(&request.extension, (SCN)44));
	env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REQUEST;
	env.payload_length = sizeof(request);
	env.source_node_id = 2;
	env.dest_node_id = 0;
	env.epoch = 7;
	UT_ASSERT_EQ(cluster_gcs_block_r4_route_cr(&env, &request, &reason), CLUSTER_CR_BUILD_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(test_open_route_forwards, 1);
	UT_ASSERT_EQ(test_open_route_refusals, 0);
	UT_ASSERT_EQ(test_open_route_forward.base.original_requester_node, 2);
	UT_ASSERT_EQ(test_open_route_forward.base.master_node, 0);
	UT_ASSERT_EQ(test_open_route_forward.base.request_id, 42);
	UT_ASSERT_EQ(GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&test_open_route_forward.base),
				 (SCN)44);
	UT_ASSERT_EQ(memcmp(&before, SemanticActivationAckTable, sizeof(before)), 0);
	test_remote_admitted_incarnations[3]++;
	request.base.request_id++;
	UT_ASSERT_EQ(cluster_gcs_block_r4_route_cr(&env, &request, &reason),
				 CLUSTER_CR_BUILD_RETRYABLE);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_RF_DEFERRED);
	UT_ASSERT_EQ(test_open_route_forwards, 1);
	UT_ASSERT_EQ(test_open_route_refusals, 1);
	UT_ASSERT_EQ(memcmp(&before, SemanticActivationAckTable, sizeof(before)), 0);
}

UT_TEST(test_r4_peer_ordinary_open_rechecks_every_member_and_feature)
{
	ClusterSemanticAdmissionToken token;
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	int leg;

	for (leg = 0; leg < 12; leg++) {
		ut_resource_x_open_carrier_setup(&token);
		token.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		UT_ASSERT(cluster_semantic_activation_peer_open_matches(
			&token, 2, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 19));
		switch (leg) {
		case 0:
			table->target_feature_bitmap &= ~token.feature_bit;
			break;
		case 1:
			table->observed_members_lo &= ~UINT64_C(8);
			break;
		case 2:
			table->observed[3].boot_id++;
			break;
		case 3:
			test_remote_admitted_incarnations[3]++;
			break;
		case 4:
			table->record_generation++;
			break;
		case 5:
			table->transition_epoch++;
			break;
		case 6:
			table->flags &= ~CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
			break;
		case 7:
			table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
			break;
		case 8:
			table->rollback_feature_bitmap = token.feature_bit;
			break;
		case 9:
			test_peer_capability_matches = false;
			break;
		case 10:
			test_membership_snapshot_valid = false;
			break;
		case 11:
			table->publication_seq.value |= 1;
			break;
		}
		UT_ASSERT(!cluster_semantic_activation_peer_open_matches(
			&token, 2, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 19));
	}
	ut_resource_x_open_carrier_setup(&token);
	token.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	table->source_feature_bitmap = 0;
	table->target_feature_bitmap = token.feature_bit;
	test_gate_publish(2, token.feature_bit, 6, 7, false);
	UT_ASSERT(cluster_semantic_activation_peer_open_matches(
		&token, 2, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 19));
	test_gate_reset();
}

/* A completed ordinary R11 OPEN_APPLIED image is the positive Resource-X
 * carrier after cutover.  A single LMON tick that cannot capture the current
 * admission snapshot must not rewrite that terminal image into an empty
 * tombstone: no contradictory identity was observed, and the next exact
 * capture still has to revalidate every member/connection row before use. */
UT_TEST(test_145h_resource_x_open_carrier_survives_unavailable_snapshot)
{
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	ClusterSemanticAdmissionToken token;

	ut_resource_x_open_carrier_setup(&token);

	/* Unavailable is not contradictory.  Preserve the terminal carrier and
	 * let every consumer remain fail-closed until a later exact capture. */
	test_membership_snapshot_valid = false;
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(table->stage,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED);
	UT_ASSERT_EQ(table->flags,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
		| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE);
	UT_ASSERT_EQ(table->expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0x0f));

	test_membership_snapshot_valid = true;
	UT_ASSERT(cluster_semantic_activation_resource_x_peer_open_matches(
		&token, 2, 19));
	test_gate_reset();
}

/* An exact snapshot with a different formation is contradictory evidence,
 * not temporary unavailability.  The completed carrier belongs to the old
 * round and must be invalidated as a whole. */
UT_TEST(test_145i_resource_x_open_carrier_invalidates_formation_drift)
{
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	ClusterSemanticAdmissionToken token;

	ut_resource_x_open_carrier_setup(&token);
	test_membership_snapshot_epoch = 8;
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(table->expected_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(table->expected_members_hi, UINT64_C(0));
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(table->observed_members_hi, UINT64_C(0));
	UT_ASSERT(!cluster_semantic_activation_resource_x_peer_open_matches(
		&token, 2, 19));
	test_gate_reset();
}

UT_TEST(test_145j_resource_x_peer_match_reports_first_failed_predicate)
{
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	ClusterSemanticAdmissionToken token;

	ut_resource_x_open_carrier_setup(&token);
	UT_ASSERT_EQ(
		cluster_semantic_activation_resource_x_peer_open_check(
			&token, 2, 19),
		CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);

	table->flags = 0;
	UT_ASSERT_EQ(
		cluster_semantic_activation_resource_x_peer_open_check(
			&token, 2, 19),
		CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_FLAGS_MISMATCH);
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				 | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;

	test_membership_snapshot_valid = false;
	UT_ASSERT_EQ(
		cluster_semantic_activation_resource_x_peer_open_check(
			&token, 2, 19),
		CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_AUTHORITY_UNAVAILABLE);
	test_membership_snapshot_valid = true;

	test_peer_capability_matches = false;
	test_peer_capability_generation++;
	UT_ASSERT_EQ(
		cluster_semantic_activation_resource_x_peer_open_check(
			&token, 2, 19),
		CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_CAPABILITY_DRIFT);
	test_gate_reset();
}

UT_TEST(test_a89_peer_open_missing_and_torn_observations_are_not_identity_drift)
{
	ClusterSemanticAdmissionToken token;
	ClusterSemanticActivationAckTableV1 *table;

	ut_resource_x_open_carrier_setup(&token);
	table = SemanticActivationAckTable;
	test_capability_missing_peer = 2;
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 2, 19),
				 CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_CAPABILITY_NOT_READY);
	UT_ASSERT(!cluster_semantic_activation_resource_x_peer_open_matches(&token, 2, 19));
	test_capability_missing_peer = -1;
	pg_atomic_write_u64(&table->publication_seq, 1);
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 2, 19),
				 CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_TABLE_TORN);
	pg_atomic_write_u64(&table->publication_seq, 2);
	SemanticActivationAckTable = NULL;
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 2, 19),
				 CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_TABLE_UNAVAILABLE);
	SemanticActivationAckTable = table;
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 2, 19),
				 CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);
	/* A valid different connection remains a contradiction, not pending. */
	test_peer_capability_matches = false;
	test_peer_capability_generation++;
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 2, 19),
				 CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_CAPABILITY_DRIFT);
	test_gate_reset();
}

UT_TEST(test_a89_peer_open_full_image_pending_and_independent_contradiction)
{
	ClusterSemanticAdmissionToken token;
	uint64 self_incarnation;
	int leg;

	ut_resource_x_open_carrier_setup(&token);
	test_capability_missing_peer = 1;
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 2, 19),
				 CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_OBSERVATION_PENDING);
	UT_ASSERT(!cluster_semantic_activation_resource_x_peer_open_matches(&token, 2, 19));
	/* A different later row must win over the earlier unobservable row. */
	test_remote_admitted_incarnations[3]++;
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 2, 19),
				 CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_NOT_CURRENT);
	test_remote_admitted_incarnations[3]--;
	test_capability_missing_peer = -1;
	self_incarnation = test_qvotec_self_incarnation;
	test_qvotec_self_incarnation = 0;
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 2, 19),
				 CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_OBSERVATION_PENDING);
	test_qvotec_self_incarnation = self_incarnation;
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 2, 19),
				 CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);
	/* Pending fields cannot hide other known fields in that same member row. */
	for (leg = 0; leg < 6; leg++) {
		ClusterSemanticActivationAckTableV1 *table;
		int node = leg < 2 ? 0 : 1;

		ut_resource_x_open_carrier_setup(&token);
		table = SemanticActivationAckTable;
		if (leg == 0) {
			test_qvotec_self_incarnation = 0;
			table->expected[0].capability_word ^= UINT32_C(1) << 31;
		} else if (leg == 1) {
			test_last_admitted_incarnation = 0;
			table->expected[0].control_connection_generation++;
		} else if (leg == 2) {
			test_remote_admitted_incarnations[1] = 0;
			table->expected[1].control_connection_generation++;
		} else {
			test_capability_missing_peer = 1;
			if (leg == 3)
				table->expected[1].boot_id++;
			if (leg == 4)
				table->expected[1].transition_epoch++;
			if (leg == 5)
				table->expected[1].record_generation++;
		}
		table->observed[node] = table->expected[node];
		UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 2, 19),
					 CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_NOT_CURRENT);
	}
	test_gate_reset();
}

/* OPEN_PROOF reconstruction owns only the empty volatile table left by a
 * postmaster restart.  A live ordinary Resource-X OPEN_APPLIED carrier is a
 * different completed round and must remain byte-identical; otherwise the
 * recovery-only flag makes its exact R11 validator reject every type-17. */
UT_TEST(test_145k_restore_open_proof_does_not_overwrite_resource_x_carrier)
{
	const uint64 r4 = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	const uint64 target_bits
		= r4 | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	ClusterSemanticActivationAckTableV1 before;
	ClusterSemanticActivationRecord open;
	ClusterSemanticAdmissionToken token;

	ut_resource_x_open_carrier_setup(&token);
	UT_ASSERT(cluster_r4_bit22_cutover_latch_apply(7, 6));
	memset(&open, 0, sizeof(open));
	open.source_feature_bitmap = r4;
	open.target_feature_bitmap = target_bits;
	open.transition_epoch = 7;
	open.record_generation = 6;
	open.admitted_members_lo = UINT64_C(0x0f);
	open.capability_sample_digest = UINT64_C(0xabc123);
	open.coordinator_incarnation = test_qvotec_self_incarnation;
	open.coordinator_node = 0;
	open.phase = CLUSTER_SEMANTIC_PHASE_OPEN;
	test_r4fsm_bootstrap_read_result = CLUSTER_SEMANTIC_ACTIVATION_OK;
	test_r4fsm_bootstrap_read_ok = true;
	test_r4fsm_bootstrap_implicit_open = false;
	UT_ASSERT(cluster_semantic_activation_record_encode(
		&open, test_r4fsm_bootstrap_bytes));
	memcpy(&before, SemanticActivationAckTable, sizeof(before));

	UT_ASSERT(!cluster_semantic_activation_restore_open_proof_if_active());
	UT_ASSERT_EQ(memcmp(&before, SemanticActivationAckTable,
					   sizeof(before)), 0);
	UT_ASSERT(cluster_semantic_activation_resource_x_peer_open_matches(
		&token, 2, 19));
	test_gate_reset();
}

/* The approved initial-clean four-node formation uses transition epoch zero.
 * Its completed ordinary Resource-X OPEN_APPLIED carrier has the same
 * terminal retention contract as a later nonzero formation: a temporarily
 * unavailable coherent QVOTEC snapshot cannot erase it. */
UT_TEST(test_145l_epoch_zero_resource_x_open_carrier_survives_unavailable_snapshot)
{
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	ClusterSemanticAdmissionToken token;

	ut_resource_x_open_carrier_setup_at_epoch(&token, 0);
	test_membership_snapshot_valid = false;
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(table->stage,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED);
	UT_ASSERT_EQ(table->flags,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
		| CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE);
	UT_ASSERT_EQ(table->expected_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(table->observed_members_lo, UINT64_C(0x0f));

	test_membership_snapshot_valid = true;
	UT_ASSERT(cluster_semantic_activation_resource_x_peer_open_matches(
		&token, 2, 19));
	test_gate_reset();
}

/* A coherent snapshot can be briefly unavailable while QVOTEC publishes its
 * next observation.  Once the same utility round has installed durable
 * PREPARE and closed source admission, absence is not evidence of identity
 * drift: retain the exact in-progress carrier so the next exact observation
 * can resume SAMPLE -> BARRIER instead of stranding transition_closed. */
UT_TEST(test_145m_resource_x_prepare_carrier_survives_unavailable_snapshot)
{
	const uint64 r4 = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	ClusterSemanticActivationAckTableV1 before;
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	ClusterSemanticAdmissionToken token;
	int node;

	ut_resource_x_open_carrier_setup(&token);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				 | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->source_feature_bitmap = r4;
	table->capability_sample_digest = 0;
	for (node = 0; node < 4; node++) {
		table->expected[node].record_generation = table->record_generation;
		table->observed[node].record_generation = table->record_generation;
	}
	test_gate_publish(2, r4, table->record_generation,
				  table->transition_epoch, true);
	semantic_activation_ack_local_request_origin.active = true;
	semantic_activation_lmon_prepare_cas_seq = 41;
	semantic_activation_lmon_prepare_cas_utility_request_seq
		= table->round_nonce;
	memcpy(&before, table, sizeof(before));

	test_membership_snapshot_valid = false;
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(memcmp(&before, table, sizeof(before)), 0);
	UT_ASSERT(semantic_activation_ack_local_request_origin.active);
	UT_ASSERT_EQ(semantic_activation_lmon_prepare_cas_seq, UINT64_C(41));

	test_membership_snapshot_valid = true;
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(memcmp(&before, table, sizeof(before)), 0);
	test_gate_reset();
}

/* Drive the real utility/CAS helpers through the same durable-PREPARE
 * boundary as a second activation. Only external membership observations
 * are supplied by the existing unit-test dependency stubs. */
static void
ut_resource_x_prepare_utility_setup(SemanticActivationUtilityRequest *request, bool install_prepare)
{
	ClusterSemanticAdmissionToken token;
	ClusterSemanticActivationAckTableV1 *table = SemanticActivationAckTable;
	SemanticActivationAdmissionSnapshot snapshot;
	ClusterSemanticActivationCasRequest cas;
	uint64 request_seq;
	int node;

	ut_resource_x_open_carrier_setup_at_epoch(&token, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	table->record_generation = 4;
	table->capability_sample_digest = 0;
	for (node = 0; node < 4; node++) {
		table->expected[node].record_generation = 4;
		table->observed[node].record_generation = 4;
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
	}
	test_gate_publish(2, table->source_feature_bitmap, 3, 0, false);
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, table->source_feature_bitmap, table->target_feature_bitmap, 0,
		3, &request_seq));
	table->round_nonce = request_seq;
	semantic_activation_ack_local_request_origin.active = true;
	UT_ASSERT(semantic_activation_utility_mailbox_poll(request));
	UT_ASSERT(semantic_activation_snapshot(&snapshot));
	UT_ASSERT(semantic_activation_ack_lmon_submit_prepare(request, &snapshot));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_cas(&cas));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_cas(
		cas.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
	if (install_prepare) {
		UT_ASSERT(semantic_activation_ack_lmon_install_prepare(request));
		UT_ASSERT(semantic_activation_snapshot(&snapshot));
		UT_ASSERT(snapshot.transition_closed);
		UT_ASSERT_EQ(snapshot.record_generation, UINT64_C(4));
	}
	test_resource_x_gate_snapshot_valid = true;
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_FROZEN;
	test_resource_x_gate_snapshot.formation = 17;
	test_resource_x_gate_snapshot.freeze_generation = 1;
	test_resource_x_cutover_digest_valid = true;
	test_resource_x_cutover_token.old_formation = 17;
	test_resource_x_cutover_token.new_formation = 18;
	test_resource_x_cutover_token.freeze_generation = 1;
	test_resource_x_cutover_digest = UINT64_C(0xa55a9911);
}

UT_TEST(test_145n_prepare_utility_has_real_pending_owner_after_projection)
{
	SemanticActivationUtilityRequest request;
	SemanticActivationUtilityRequest pending = { 0 };

	ut_resource_x_prepare_utility_setup(&request, true);
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending));
	UT_ASSERT_EQ(pending.request_seq, request.request_seq);
	test_gate_reset();
}

UT_TEST(test_145o_unavailable_membership_cannot_complete_prepared_utility)
{
	SemanticActivationUtilityRequest request;
	SemanticActivationUtilityRequest pending = { 0 };
	ClusterSemanticActivationAckTableV1 before;

	ut_resource_x_prepare_utility_setup(&request, true);
	UT_ASSERT(semantic_activation_ack_table_snapshot(&before));
	test_membership_snapshot_valid = false;
	cluster_semantic_activation_lmon_tick();
	/* The carrier-only test145m is insufficient: its utility must also stay
	 * owned, so a later exact observation can drive the original round. */
	UT_ASSERT_EQ(memcmp(&before, SemanticActivationAckTable, sizeof(before)), 0);
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending));
	UT_ASSERT_EQ(pending.request_seq, request.request_seq);
	test_membership_snapshot_valid = true;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(SemanticActivationAckTable->stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending));
	test_gate_reset();
}

UT_TEST(test_145p_completed_prepare_cas_keeps_owner_before_local_projection)
{
	SemanticActivationUtilityRequest request;
	SemanticActivationUtilityRequest pending = { 0 };
	ClusterSemanticActivationAckTableV1 before;
	uint64 cas_seq;

	ut_resource_x_prepare_utility_setup(&request, false);
	cas_seq = semantic_activation_lmon_prepare_cas_seq;
	UT_ASSERT(semantic_activation_ack_table_snapshot(&before));
	test_membership_snapshot_valid = false;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(memcmp(&before, SemanticActivationAckTable, sizeof(before)), 0);
	UT_ASSERT_EQ(semantic_activation_lmon_prepare_cas_seq, cas_seq);
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending));
	test_membership_snapshot_valid = true;
	cluster_semantic_activation_lmon_tick();
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(SemanticActivationAckTable->stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending));
	test_gate_reset();
}

UT_TEST(test_145q_unavailable_observation_does_not_hide_identity_contradiction)
{
	SemanticActivationUtilityRequest request;
	ClusterSemanticActivationRefusal refusal;
	int variant;

	for (variant = 0; variant < 4; variant++) {
		ut_resource_x_prepare_utility_setup(&request, true);
		test_membership_snapshot_valid = false;
		if (variant == 0)
			test_current_epoch++;
		else if (variant == 1)
			test_remote_admitted_incarnations[2]++;
		else if (variant == 2)
			SemanticActivationAckTable->observed[2].record_generation++;
		else
			pg_atomic_write_u32(&SemanticActivationShmem->record_cas_result,
								CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT);
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT(
			semantic_activation_utility_mailbox_poll_completion(request.request_seq, &refusal));
		UT_ASSERT(refusal.result != CLUSTER_SEMANTIC_ACTIVATION_OK);
	}
	test_gate_reset();
}

UT_TEST(test_145u_cas_binding_copy_contradiction_cannot_retain_utility)
{
	SemanticActivationUtilityRequest request;
	int variant;

	for (variant = 0; variant < 2; variant++) {
		ClusterSemanticActivationRefusal refusal = { 0 };
		ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_OK;
		SemanticActivationAdmissionSnapshot before;
		SemanticActivationAdmissionSnapshot after;

		ut_resource_x_prepare_utility_setup(&request, false);
		UT_ASSERT(semantic_activation_snapshot(&before));
		if (variant == 0)
			SemanticActivationUtilityMailbox->utility_result_feature_bit++;
		else
			SemanticActivationUtilityMailbox->utility_result_expected_generation++;
		UT_ASSERT(semantic_activation_record_cas_mailbox_poll_completion(
			semantic_activation_lmon_prepare_cas_seq, &result));
		UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
		semantic_activation_lmon_consume_utility();
		UT_ASSERT(
			semantic_activation_utility_mailbox_poll_completion(request.request_seq, &refusal));
		UT_ASSERT(refusal.result != CLUSTER_SEMANTIC_ACTIVATION_OK);
		UT_ASSERT(semantic_activation_snapshot(&after));
		/* Snapshot padding is not part of the published gate contract. */
		UT_ASSERT_EQ(before.seq, after.seq);
		UT_ASSERT_EQ(before.active_bits, after.active_bits);
		UT_ASSERT_EQ(before.record_generation, after.record_generation);
		UT_ASSERT_EQ(before.formation_epoch, after.formation_epoch);
		UT_ASSERT_EQ(before.transition_closed, after.transition_closed);
	}
	test_gate_reset();
}

UT_TEST(test_145r_each_mid_helper_membership_gap_preserves_same_owner)
{
	SemanticActivationUtilityRequest request;
	SemanticActivationUtilityRequest pending = { 0 };
	int calls;
	int start;
	int offset;

	ut_resource_x_prepare_utility_setup(&request, true);
	start = test_membership_snapshot_calls;
	semantic_activation_lmon_consume_utility();
	calls = test_membership_snapshot_calls - start;
	UT_ASSERT(calls > 1);
	UT_ASSERT_EQ(SemanticActivationAckTable->stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	for (offset = 1; offset <= calls; offset++) {
		ut_resource_x_prepare_utility_setup(&request, true);
		test_membership_snapshot_fail_at_call = test_membership_snapshot_calls + offset;
		semantic_activation_lmon_consume_utility();
		UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending));
		UT_ASSERT_EQ(pending.request_seq, request.request_seq);
		test_membership_snapshot_fail_at_call = 0;
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(SemanticActivationAckTable->stage,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	}
	test_gate_reset();
}

UT_TEST(test_145s_capability_gap_waits_but_other_identity_contradiction_wins)
{
	SemanticActivationUtilityRequest request;
	SemanticActivationUtilityRequest pending = { 0 };
	ClusterSemanticActivationRefusal refusal;

	ut_resource_x_prepare_utility_setup(&request, true);
	test_capability_missing_peer = 2;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending));
	test_capability_missing_peer = -1;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(SemanticActivationAckTable->stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);

	ut_resource_x_prepare_utility_setup(&request, true);
	test_capability_missing_peer = 1;
	test_remote_admitted_incarnations[2]++;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(request.request_seq, &refusal));
	UT_ASSERT(refusal.result != CLUSTER_SEMANTIC_ACTIVATION_OK);
	test_gate_reset();
}

static void
ut_r11_ack_all_current(void)
{
	ClusterSemanticActivationAckTableV1 table;
	int node;

	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	for (node = 1; node < 4; node++) {
		ClusterSemanticActivationAckWireV1 ack = { 0 };
		ClusterICEnvelope envelope = { 0 };
		uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

		ack.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
		ack.stage = table.stage;
		ack.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
		ack.coordinator_node = 0;
		ack.member_node = (uint32)node;
		ack.transition_epoch = table.transition_epoch;
		ack.record_generation = table.record_generation;
		ack.round_nonce = table.round_nonce;
		ack.source_feature_bitmap = table.source_feature_bitmap;
		ack.target_feature_bitmap = table.target_feature_bitmap;
		ack.admitted_members_lo = UINT64_C(0x0f);
		ack.capability_sample_digest = table.capability_sample_digest;
		ack.boot_id = test_remote_admitted_incarnations[node];
		ack.admitted_incarnation = ack.boot_id;
		ack.capability_word = test_peer_capability_word;
		UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&ack, payload));
		envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
		envelope.source_node_id = (uint32)node;
		envelope.dest_node_id = 0;
		envelope.epoch = test_current_epoch;
		envelope.payload_length = sizeof(payload);
		cluster_semantic_activation_ack_handler(&envelope, payload);
	}
}

UT_TEST(test_145t_owned_commit_and_open_resume_without_a_new_utility)
{
	SemanticActivationUtilityRequest request;
	SemanticActivationUtilityRequest pending = { 0 };
	ClusterSemanticActivationCasRequest cas;
	ClusterSemanticActivationRecord desired;
	ClusterSemanticActivationRefusal refusal;
	SemanticActivationAdmissionSnapshot snapshot;
	int phase;

	ut_resource_x_prepare_utility_setup(&request, true);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(SemanticActivationAckTable->stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	ut_r11_ack_all_current();
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(SemanticActivationAckTable->stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	cluster_semantic_activation_lmon_tick();
	ut_r11_ack_all_current();
	cluster_semantic_activation_lmon_tick();
	for (phase = CLUSTER_SEMANTIC_PHASE_COMMIT; phase <= CLUSTER_SEMANTIC_PHASE_OPEN; phase++) {
		UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_cas(&cas));
		UT_ASSERT(cluster_semantic_activation_record_decode(cas.desired_bytes, &desired, NULL));
		UT_ASSERT_EQ(desired.phase, phase);
		/* The CAS is not yet complete. A missing observation cannot complete
		 * or replace its utility, nor authorize any projection. */
		test_membership_snapshot_valid = false;
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending));
		UT_ASSERT_EQ(pending.request_seq, request.request_seq);
		test_membership_snapshot_valid = true;
		UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_cas(
			cas.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(SemanticActivationAckTable->stage,
					 phase == CLUSTER_SEMANTIC_PHASE_COMMIT
						 ? CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED
						 : CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED);
		test_membership_snapshot_valid = false;
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT(semantic_activation_utility_mailbox_poll(&pending));
		test_membership_snapshot_valid = true;
		if (phase == CLUSTER_SEMANTIC_PHASE_OPEN)
			test_resource_x_cutover_thawed = true;
		cluster_semantic_activation_lmon_tick();
		ut_r11_ack_all_current();
		cluster_semantic_activation_lmon_tick();
	}
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(request.request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(semantic_activation_snapshot(&snapshot));
	UT_ASSERT(!snapshot.transition_closed);
	UT_ASSERT_EQ(snapshot.record_generation, UINT64_C(6));
	UT_ASSERT_EQ(snapshot.active_bits, UINT64_C(0x401));
	test_gate_reset();
}

/* A clean new incarnation must send a real recovery OPEN request. A durable
 * digest is not a substitute for the peers' new observations. */
UT_TEST(test_a142_clean_restart_requests_fresh_open_while_closed)
{
	ClusterSemanticAdmissionToken unused;
	ClusterSemanticActivationRecord open;
	ClusterSemanticActivationReadRequest read;
	ClusterSemanticActivationAckWireV1 request;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 generation;
	int node;

	ut_resource_x_open_carrier_setup_at_epoch(&unused, 0);
	memset(SemanticActivationAckTable, 0, sizeof(*SemanticActivationAckTable));
	test_gate_publish(0, 0, 0, 0, true);
	pg_atomic_write_u32(&test_semantic_clean_start, 1);
	test_initial_clean_snapshot_valid = true;
	memset(&test_initial_clean_snapshot, 0, sizeof(test_initial_clean_snapshot));
	test_initial_clean_snapshot.members_lo = UINT64_C(15);
	for (node = 0; node < 4; node++) {
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
		test_initial_clean_snapshot.admitted_incarnation[node]
			= cluster_membership_get_last_admitted_incarnation(node);
	}
	memset(&open, 0, sizeof(open));
	open.phase = CLUSTER_SEMANTIC_PHASE_OPEN;
	open.record_generation = 6;
	open.source_feature_bitmap = 1;
	open.target_feature_bitmap = 1025;
	open.admitted_members_lo = 15;
	open.coordinator_incarnation = 99; /* historical, not the new boot */
	open.capability_sample_digest = UINT64_C(0xabc123);
	UT_ASSERT(cluster_semantic_activation_record_encode(&open, bytes));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(&read));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		read.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, false, bytes));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED);
	UT_ASSERT_EQ(cluster_resource_x_writer_path_snapshot(&generation), RESOURCE_X_WRITER_CLOSED);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, UINT64_C(0));
	for (node = 1; node < 4; node++) {
		UT_ASSERT_EQ(test_send_calls[node], 1);
		UT_ASSERT(cluster_semantic_activation_ack_wire_decode(test_send_payloads[node], &request));
		UT_ASSERT_EQ(request.kind, CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST);
		UT_ASSERT_EQ(request.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED);
		UT_ASSERT_EQ(request.round_nonce, read.request_seq);
	}
	test_gate_reset();
}

static void
ut_a142_setup(int node)
{
	ClusterSemanticAdmissionToken unused;
	ClusterUndoRootDescriptorV1 root;
	int peer;

	ut_resource_x_open_carrier_setup_at_epoch(&unused, 0);
	cluster_node_id = node;
	test_qvotec_self_incarnation = UINT64_C(0x100) + node;
	test_last_admitted_incarnation = test_qvotec_self_incarnation;
	memset(SemanticActivationAckTable, 0, sizeof(*SemanticActivationAckTable));
	test_gate_publish(0, 0, 0, 0, true);
	cluster_semantic_activation_note_clean_start(true);
	test_initial_clean_snapshot_valid = true;
	memset(&test_initial_clean_snapshot, 0, sizeof(test_initial_clean_snapshot));
	test_initial_clean_snapshot.members_lo = 15;
	for (peer = 0; peer < 4; peer++) {
		test_send_results[peer] = CLUSTER_IC_SEND_DONE;
		test_initial_clean_snapshot.admitted_incarnation[peer] = UINT64_C(0x100) + peer;
	}
	test_system_identifier = UINT64_C(0x8070605040302010);
	cluster_shared_data_dir = "/cluster-share";
	memset(&root, 0, sizeof(root));
	root.descriptor_incarnation = 1;
	root.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	root.owner_node = -1;
	memset(root.root_uuid, 0x6b, sizeof(root.root_uuid));
	root.namespace_id = 1;
	root.system_identifier = test_system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(&root, test_pgrd_candidate));
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
}

static void
ut_a142_open_bytes(uint8 *bytes, int fault)
{
	ClusterSemanticActivationRecord open;

	memset(&open, 0, sizeof(open));
	open.phase = fault == 1 ? CLUSTER_SEMANTIC_PHASE_COMMIT : CLUSTER_SEMANTIC_PHASE_OPEN;
	open.record_generation = 6;
	open.source_feature_bitmap = 1;
	open.target_feature_bitmap = 1025;
	open.admitted_members_lo = fault == 2 ? 7 : 15;
	open.coordinator_incarnation = fault == 3 ? UINT64_C(0x100) : 99;
	open.capability_sample_digest = UINT64_C(0xabc123);
	UT_ASSERT(cluster_semantic_activation_record_encode(&open, bytes));
}

static uint64
ut_a142_complete_open_read(int fault)
{
	ClusterSemanticActivationReadRequest read;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	memset(&read, 0, sizeof(read));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(&read));
	ut_a142_open_bytes(bytes, fault);
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		read.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, false, bytes));
	cluster_semantic_activation_lmon_tick();
	return read.request_seq;
}

static void
ut_a142_frame(int source, bool ack, uint64 nonce, uint64 incarnation)
{
	ClusterSemanticActivationAckWireV1 message;
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	memset(&message, 0, sizeof(message));
	message.kind = ack ? CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
					   : CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
	message.result = ack ? CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK : 0;
	message.member_node = ack ? source : cluster_node_id;
	message.record_generation = 6;
	message.round_nonce = nonce;
	message.source_feature_bitmap = 1;
	message.target_feature_bitmap = 1025;
	message.admitted_members_lo = 15;
	message.capability_sample_digest = UINT64_C(0xabc123);
	if (ack) {
		message.boot_id = incarnation;
		message.admitted_incarnation = incarnation;
		message.capability_word = test_peer_capability_word;
	}
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&message, payload));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = source;
	envelope.dest_node_id = cluster_node_id;
	envelope.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
}

static void
ut_a142_local_root(bool mismatch)
{
	ClusterUndoRootDescriptorReadRequest read;
	uint8 bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];

	cluster_semantic_activation_lmon_tick();
	memset(&read, 0, sizeof(read));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_undo_root_descriptor_read(&read));
	memcpy(bytes, test_pgrd_candidate, sizeof(bytes));
	if (mismatch)
		test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_ABSENT;
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_undo_root_descriptor_read(
		read.request_seq, CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID, bytes));
	cluster_semantic_activation_lmon_tick();
}

UT_TEST(test_a142_clean_start_observation_is_not_rearmable)
{
	ut_a142_setup(0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_semantic_clean_start), 1);
	cluster_semantic_activation_note_clean_start(false);
	cluster_semantic_activation_note_clean_start(true);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_semantic_clean_start), 2);
	test_gate_reset();
}

UT_TEST(test_a142_unsafe_or_missing_startup_never_requests_open)
{
	int fault;
	for (fault = 0; fault < 4; fault++) {
		ut_a142_setup(0);
		if (fault == 0)
			cluster_semantic_activation_note_clean_start(false);
		if (fault == 1)
			test_restart_unclean = true;
		if (fault == 2)
			test_restart_in_recovery = true;
		if (fault == 3)
			pg_atomic_write_u32(&test_semantic_clean_start, 0);
		cluster_semantic_activation_lmon_tick();
		(void)ut_a142_complete_open_read(0);
		UT_ASSERT_EQ(test_send_calls[1], 0);
		UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, UINT64_C(0));
		UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	}
	test_gate_reset();
}

UT_TEST(test_a142_wrong_durable_phase_member_or_old_boot_stays_closed)
{
	int fault;
	for (fault = 1; fault <= 3; fault++) {
		ut_a142_setup(0);
		cluster_semantic_activation_lmon_tick();
		(void)ut_a142_complete_open_read(fault);
		UT_ASSERT(semantic_activation_restart.failed);
		UT_ASSERT_EQ(test_send_calls[1], 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	}
	test_gate_reset();
}

UT_TEST(test_a142_root_mismatch_cannot_emit_a_self_ack)
{
	ut_a142_setup(0);
	cluster_semantic_activation_lmon_tick();
	(void)ut_a142_complete_open_read(0);
	ut_a142_local_root(true);
	UT_ASSERT(!semantic_activation_restart.local_ready);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(test_send_calls[1], 1); /* REQUEST only */
	test_gate_reset();
}

UT_TEST(test_a142_missing_or_old_ack_never_opens_target)
{
	ClusterSemanticR11CutoverSnapshot cutover;
	uint64 nonce;

	ut_a142_setup(0);
	cluster_semantic_activation_lmon_tick();
	nonce = ut_a142_complete_open_read(0);
	ut_a142_local_root(false);
	ut_a142_frame(1, true, nonce, UINT64_C(0x101));
	ut_a142_frame(2, true, nonce, UINT64_C(0x102));
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, UINT64_C(7));
	UT_ASSERT(!cluster_semantic_activation_r11_cutover_snapshot(&cutover));
	ut_a142_frame(3, true, nonce, 33); /* real identity contradiction */
	UT_ASSERT(semantic_activation_restart.failed);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	test_gate_reset();
}

UT_TEST(test_a142_early_ack_waits_for_directed_request_and_local_root)
{
	ut_a142_setup(3);
	cluster_semantic_activation_lmon_tick();
	(void)ut_a142_complete_open_read(0);
	ut_a142_frame(1, true, 23, UINT64_C(0x101));
	UT_ASSERT_EQ(SemanticActivationAckTable->expected_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(semantic_activation_restart.early.count, 1);
	ut_a142_frame(0, false, 23, 0);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, UINT64_C(2));
	UT_ASSERT_EQ(test_send_calls[0], 0);
	ut_a142_local_root(false);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, UINT64_C(10));
	UT_ASSERT_EQ(test_send_calls[0], 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	test_gate_reset();
}

UT_TEST(test_a142_actual_mailboxes_ack_fanin_and_native_proof_precede_open)
{
	ClusterSemanticR11CutoverSnapshot cutover;
	ClusterSemanticAdmissionToken token;
	uint64 nonce, generation;
	int node;

	ut_a142_setup(0);
	cluster_semantic_activation_lmon_tick();
	nonce = ut_a142_complete_open_read(0);
	ut_a142_local_root(false);
	/* Wrong nonce is stale, not an ACK for this restart. */
	ut_a142_frame(1, true, nonce + 17, UINT64_C(0x101));
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, UINT64_C(1));
	for (node = 1; node < 4; node++)
		ut_a142_frame(node, true, nonce, UINT64_C(0x100) + node);
	UT_ASSERT_EQ(SemanticActivationAckTable->flags, 3);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, UINT64_C(15));
	cluster_semantic_activation_lmon_tick();
	(void)ut_a142_complete_open_read(0); /* fresh majority read before open */
	UT_ASSERT(cluster_semantic_activation_r11_cutover_snapshot(&cutover));
	UT_ASSERT_EQ(cutover.phase, CLUSTER_SEMANTIC_R11_CUTOVER_SOURCE_CLOSED);
	UT_ASSERT_EQ(cluster_resource_x_writer_path_snapshot(&generation), RESOURCE_X_WRITER_CLOSED);
	test_resource_x_cutover_digest_valid = true;
	test_resource_x_cutover_thawed = false;
	test_resource_x_cutover_digest = 99;
	test_resource_x_cutover_token.old_formation = 1;
	test_resource_x_cutover_token.new_formation = 2;
	test_resource_x_cutover_token.freeze_generation = 1;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(cluster_semantic_activation_r11_cutover_snapshot(&cutover));
	UT_ASSERT_EQ(cutover.phase, CLUSTER_SEMANTIC_R11_CUTOVER_DURABLE_OPEN_PENDING_LOCAL);
	UT_ASSERT_EQ(cluster_resource_x_writer_path_snapshot(&generation), RESOURCE_X_WRITER_CLOSED);
	test_resource_x_cutover_thawed = true;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(cluster_resource_x_writer_path_snapshot(&generation), RESOURCE_X_WRITER_TARGET);
	UT_ASSERT_EQ(generation, UINT64_C(6));
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(
		cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
										  CLUSTER_SEMANTIC_TARGET_SIDE, &token),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(cluster_semantic_activation_resource_x_peer_open_matches(&token, 2, 19));
	cluster_semantic_activation_leave(&token);
	test_gate_reset();
}

UT_TEST(test_a142_early_ack_duplicate_is_idempotent_but_conflict_refuses)
{
	ut_a142_setup(3);
	cluster_semantic_activation_lmon_tick();
	(void)ut_a142_complete_open_read(0);
	ut_a142_frame(1, true, 23, UINT64_C(0x101));
	ut_a142_frame(1, true, 23, UINT64_C(0x101));
	UT_ASSERT_EQ(semantic_activation_restart.early.count, 1);
	UT_ASSERT(!semantic_activation_restart.failed);
	ut_a142_frame(1, true, 24, UINT64_C(0x101));
	UT_ASSERT(semantic_activation_restart.failed);
	ut_a142_frame(0, false, 23, 0);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	test_gate_reset();
}

UT_TEST(test_a142_current_member_change_prevents_native_projection)
{
	ClusterSemanticR11CutoverSnapshot cutover;
	uint64 nonce;
	int node;

	ut_a142_setup(0);
	cluster_semantic_activation_lmon_tick();
	nonce = ut_a142_complete_open_read(0);
	ut_a142_local_root(false);
	for (node = 1; node < 4; node++)
		ut_a142_frame(node, true, nonce, UINT64_C(0x100) + node);
	cluster_semantic_activation_lmon_tick();
	(void)ut_a142_complete_open_read(0);
	UT_ASSERT(cluster_semantic_activation_r11_cutover_snapshot(&cutover));
	test_remote_admitted_incarnations[2]++;
	UT_ASSERT(!cluster_semantic_activation_r11_cutover_snapshot(&cutover));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	test_gate_reset();
}

int
main(void)
{
	UT_PLAN(253);
	UT_RUN(test_01_feature_bit_is_one);
	UT_RUN(test_02_required_hello_caps_are_frozen);
	UT_RUN(test_03_action_values_are_frozen);
	UT_RUN(test_04_admission_values_are_frozen);
	UT_RUN(test_05_activation_result_values_are_frozen);
	UT_RUN(test_06_admission_side_values_are_frozen);
	UT_RUN(test_07_r4_descriptor_identity);
	UT_RUN(test_08_r4_descriptor_caps_and_active_bits);
	UT_RUN(test_09_r4_descriptor_retains_source);
	UT_RUN(test_10_r4_descriptor_has_every_callback);
	UT_RUN(test_10a_r11_resource_x_cutover_descriptor_is_compiled_exact);
	UT_RUN(test_10b_r11_writer_selector_snapshots_one_exact_gate_generation);
	UT_RUN(test_10c_r4_carrier_selects_one_exact_compiled_feature_round);
	UT_RUN(test_11_source_only_is_exclusive);
	UT_RUN(test_12_target_only_is_exclusive);
	UT_RUN(test_13_enable_source_open_to_admission_stopped);
	UT_RUN(test_14_enable_admission_stopped_to_drain);
	UT_RUN(test_15_enable_drain_to_logical_zero);
	UT_RUN(test_16_enable_logical_zero_to_transport_barrier);
	UT_RUN(test_17_enable_transport_barrier_to_transport_zero);
	UT_RUN(test_18_enable_transport_zero_to_epoch_barrier);
	UT_RUN(test_19_enable_epoch_barrier_to_target_staged);
	UT_RUN(test_20_enable_target_staged_to_committed_closed);
	UT_RUN(test_21_enable_committed_closed_to_target_open);
	UT_RUN(test_22_enable_target_open_is_terminal);
	UT_RUN(test_23_disable_source_open_to_admission_stopped);
	UT_RUN(test_24_disable_admission_stopped_to_drain);
	UT_RUN(test_25_disable_drain_to_logical_zero);
	UT_RUN(test_26_disable_logical_zero_to_transport_barrier);
	UT_RUN(test_27_disable_transport_barrier_to_transport_zero);
	UT_RUN(test_28_disable_transport_zero_to_epoch_barrier);
	UT_RUN(test_29_disable_epoch_barrier_to_target_staged);
	UT_RUN(test_30_disable_target_staged_to_committed_closed);
	UT_RUN(test_31_disable_committed_closed_to_target_open);
	UT_RUN(test_32_disable_target_open_is_terminal);
	UT_RUN(test_33_admission_stop_calls_close_source);
	UT_RUN(test_34_drain_has_no_eraser_callback);
	UT_RUN(test_35_logical_zero_calls_logical_proof);
	UT_RUN(test_36_ordered_barrier_calls_transport_barrier);
	UT_RUN(test_37_transport_zero_calls_transport_proof);
	UT_RUN(test_38_epoch_state_calls_exact_ack_barrier);
	UT_RUN(test_39_target_staged_calls_prepare);
	UT_RUN(test_40_committed_closed_calls_apply);
	UT_RUN(test_41_target_open_calls_open_admission);
	UT_RUN(test_42_source_open_has_no_transition_callback);
	UT_RUN(test_43_source_open_has_no_self_edge);
	UT_RUN(test_44_admission_stopped_has_no_self_edge);
	UT_RUN(test_45_drain_has_no_self_edge);
	UT_RUN(test_46_logical_zero_has_no_self_edge);
	UT_RUN(test_47_transport_barrier_has_no_self_edge);
	UT_RUN(test_48_transport_zero_has_no_self_edge);
	UT_RUN(test_49_epoch_barrier_has_no_self_edge);
	UT_RUN(test_50_target_staged_has_no_self_edge);
	UT_RUN(test_51_committed_closed_has_no_self_edge);
	UT_RUN(test_52_target_open_has_no_self_edge);
	UT_RUN(test_53_invalid_low_state_has_no_edge);
	UT_RUN(test_54_invalid_high_state_has_no_edge);
	UT_RUN(test_55_failure_at_source_open_restores_source);
	UT_RUN(test_56_failure_after_admission_stop_restores_source);
	UT_RUN(test_57_failure_during_drain_restores_source);
	UT_RUN(test_58_failure_after_logical_zero_restores_source);
	UT_RUN(test_59_failure_during_ordered_barrier_restores_source);
	UT_RUN(test_60_failure_after_transport_zero_restores_source);
	UT_RUN(test_61_failure_at_epoch_barrier_restores_source);
	UT_RUN(test_62_failure_after_prepare_restores_source);
	UT_RUN(test_63_failure_after_commit_requires_revert_closed);
	UT_RUN(test_64_target_open_is_not_reinterpreted_as_transition_failure);
	UT_RUN(test_65_identical_ack_tuple_matches);
	UT_RUN(test_66_node_change_invalidates_ack);
	UT_RUN(test_67_boot_change_invalidates_ack);
	UT_RUN(test_68_incarnation_change_invalidates_ack);
	UT_RUN(test_69_control_reconnect_invalidates_ack);
	UT_RUN(test_70_capability_word_change_invalidates_ack);
	UT_RUN(test_71_capability_generation_change_invalidates_ack);
	UT_RUN(test_72_epoch_change_invalidates_ack);
	UT_RUN(test_73_record_generation_change_invalidates_ack);
	UT_RUN(test_74_null_observed_ack_never_matches);
	UT_RUN(test_75_null_expected_ack_never_matches);
	UT_RUN(test_75a_d13_full_ack_table_requires_exact_member_set_and_tuples);
	UT_RUN(test_76_inactive_feature_source_is_admitted);
	UT_RUN(test_77_inactive_feature_target_is_disabled);
	UT_RUN(test_78_active_feature_source_is_dormant);
	UT_RUN(test_79_active_feature_target_is_admitted);
	UT_RUN(test_80_transition_closes_source_admission);
	UT_RUN(test_81_transition_closes_target_admission);
	UT_RUN(test_82_source_generation_change_is_typed);
	UT_RUN(test_83_target_generation_change_is_typed);
	UT_RUN(test_84_unknown_side_is_closed);
	UT_RUN(test_85_zero_feature_is_closed);
	UT_RUN(test_85a_modifier_gate_requires_durable_source_or_target_open);
	UT_RUN(test_85b_modifier_gate_closes_replacement_until_uniform_open);
	UT_RUN(test_86_r4a_snapshot_is_fixed_false);
	UT_RUN(test_87_readiness_adapter_returns_rf_deferred);
	UT_RUN(test_88_readiness_adapter_names_r4_feature);
	UT_RUN(test_89_readiness_adapter_preserves_expected_generation);
	UT_RUN(test_89d_local_ready_alone_cannot_enter_prepare);
	UT_RUN(test_89e_malformed_local_ready_remains_typed_deferred);
	UT_RUN(test_89a_d13_prepare_basis_accepts_only_exact_admitted_ready_lineage);
	UT_RUN(test_89b_d13_prepare_basis_requires_admitted_ready_polarity);
	UT_RUN(test_89c_d13_prepare_basis_rejects_generation_or_identity_drift);
	UT_RUN(test_89f_positive_ready_stays_deferred_until_full_d13_conjunction);
	UT_RUN(test_89g_d13_current_coordinator_handoff_consumes_exact_admitted_basis);
	UT_RUN(test_89h_pre_prepare_consumes_durable_admitted_not_ready_getter);
	UT_RUN(test_89h1_pre_prepare_refuses_when_ctrc_storage_is_not_ready);
	UT_RUN(test_89i_d13_invalidator_rescan_accepts_only_same_settled_closed_head);
	UT_RUN(test_90_descriptor_uses_the_only_r4a_adapter);
	UT_RUN(test_90aa_r4_logical_zero_requires_exact_closed_source_gate);
	UT_RUN(test_90ab_r4_logical_zero_refuses_live_source_debt);
	UT_RUN(test_90ac_r4_logical_zero_rechecks_gate_before_proof);
	UT_RUN(test_90a_r4_transport_zero_requires_exact_closed_gate);
	UT_RUN(test_90b_r4_transport_zero_refuses_target_debt_and_missing_drain);
	UT_RUN(test_90c_r4_transport_zero_refuses_partial_close_conjunction);
	UT_RUN(test_90d_r4_transport_zero_publishes_only_full_zero_proof);
	UT_RUN(test_91_preflight_refusal_is_before_every_mutation);
	UT_RUN(test_92_preflight_refusal_names_condition_feature);
	UT_RUN(test_93_preflight_rejects_bad_action_without_effects);
	UT_RUN(test_93a_activation_actor_effect_ownership_is_exact);
	UT_RUN(test_93b_activation_mailbox_route_has_no_owner_bypass);
	UT_RUN(test_93c_utility_mailbox_preserves_exact_owner_tuple_and_completion);
	UT_RUN(test_93d_formation_lmon_alone_consumes_utility_request);
	UT_RUN(test_93da_coordinator_begins_exact_four_node_sample_round);
	UT_RUN(test_93da0_early_sample_without_common_view_sends_one_directed_refusal);
	UT_RUN(test_93da0_exact_refusal_completes_active_utility_as_deferred);
	UT_RUN(test_93da1_member_retains_sample_ack_until_exact_request_arrives);
	UT_RUN(test_93da2_member_retains_next_round_sample_ack_until_exact_request_arrives);
	UT_RUN(test_93da2a_next_sample_waits_for_exact_local_open_publish);
	UT_RUN(test_93da3_member_retains_barrier_request_and_fans_out_once);
	UT_RUN(test_93da4_member_direct_barrier_request_waits_for_lifecycle);
	UT_RUN(test_g3_ack_complete_matches_round_binding);
	UT_RUN(test_93daa_member_accumulates_sample_and_closes_barrier);
	UT_RUN(test_93db_coordinator_reaches_exact_prepared_origin);
	UT_RUN(test_93dc_source_removed_round_waits_without_reviving_source);
	UT_RUN(test_93dca_source_removed_open_target_remains_selectable);
	UT_RUN(test_93e_utility_wait_returns_only_matching_terminal_result);
	UT_RUN(test_93ea_utility_wait_does_not_synthesize_elapsed_terminal);
	UT_RUN(test_93f_pgsa_read_mailbox_round_trip_is_qvotec_owned);
	UT_RUN(test_93fa_wrong_read_completion_cannot_mutate_pending_cas);
	UT_RUN(test_93fb_out_of_quorum_qvotec_rejects_pending_record_cas);
	UT_RUN(test_93fc_epoch_drift_qvotec_rejects_pending_record_cas);
	UT_RUN(test_93fd_incarnation_drift_qvotec_rejects_pending_record_cas);
	UT_RUN(test_93fe_record_cas_submit_requires_pending_formation);
	UT_RUN(test_93ff_utility_slot_drift_rejects_pending_record_cas);
	UT_RUN(test_93fg_admitted_basis_drift_rejects_pending_record_cas);
	UT_RUN(test_93fh_utility_expected_generation_drift_rejects_pending_record_cas);
	UT_RUN(test_93fi_pgsa_generation_drift_rejects_pending_record_cas);
	UT_RUN(test_94_rf_deferred_enable_may_drive_only_preopen_pgrd_setup);
	UT_RUN(test_94a_public_submit_cannot_bypass_busy_lmon_mailbox);
	UT_RUN(test_94ab_initial_clean_candidate_can_enter_sample_without_replacement);
	UT_RUN(test_94ac_initial_clean_prepare_waits_for_exact_full_sample_ack);
	UT_RUN(test_94ad_initial_clean_basis_is_revalidated_for_commit_and_open);
	UT_RUN(test_94ae_initial_clean_stage_callbacks_require_exact_current_basis);
	UT_RUN(test_94b_utility_cannot_close_source_before_prepare_commit);
	UT_RUN(test_95_dormant_target_enter_has_no_token);
	UT_RUN(test_96_source_token_recheck_and_leave_are_generation_scoped);
	UT_RUN(test_97_old_epoch_completion_is_inert_and_requires_revalidation);
	UT_RUN(test_98_admission_token_has_frozen_natural_layout);
	UT_RUN(test_99_shared_gate_layout_and_bootstrap_are_fail_closed);
	UT_RUN(test_99a_existing_ack_table_is_preserved_on_attach);
	UT_RUN(test_100_source_enter_owns_shared_debt_and_epoch_token);
	UT_RUN(test_100a_modifier_bootstrap_source_requires_ordinary_write_gate);
	UT_RUN(test_100b_modifier_bootstrap_source_refuses_replacement_closed_member);
	UT_RUN(test_101_active_source_refuses_before_debt);
	UT_RUN(test_102_inactive_target_refuses_before_debt);
	UT_RUN(test_102a_terminal_census_is_the_only_inactive_target_exception);
	UT_RUN(test_103_epoch_drift_invalidates_recheck_without_losing_debt);
	UT_RUN(test_104_close_invalidates_recheck_and_leave_balances_once);
	UT_RUN(test_105_pid_change_discards_inherited_local_ledger_only);
	UT_RUN(test_106_exit_hook_drains_both_side_ledgers);
	UT_RUN(test_107_odd_snapshot_is_bounded_closed_without_debt);
	UT_RUN(test_108_nonregistered_feature_is_closed_without_debt);
	UT_RUN(test_109_lmon_without_validated_majority_remains_closed);
	UT_RUN(test_109a_lmon_publishes_source_open_only_after_majority_legacy_zero);
	UT_RUN(test_109b_lmon_legacy_zero_requires_coherent_admitted_membership);
	UT_RUN(test_109c_lmon_legacy_zero_rejects_membership_epoch_drift);
	UT_RUN(test_110_lmon_odd_writer_remains_fail_closed);
	UT_RUN(test_111_formation_change_closes_before_debt_drain);
	UT_RUN(test_111a_close_source_publishes_closed_before_debt_result);
	UT_RUN(test_112_enter_samples_second_snapshot_before_epoch);
	UT_RUN(test_113_recheck_samples_snapshot_before_epoch);
	UT_RUN(test_114_peer_open_matcher_stays_closed_until_d13_ack_table);
	UT_RUN(test_115_peer_open_matcher_rejects_invalid_inputs_before_capability_match);
	UT_RUN(test_116_formation_lmon_consumes_phase3_handoff);
	UT_RUN(test_117_formation_lmon_submits_exact_mirror_candidate_to_qvotec);
	UT_RUN(test_118_formation_lmon_waits_for_pgrd_majority_before_carrier);
	UT_RUN(test_119_formation_lmon_reads_zero_quorum_before_fresh_pgrd);
	UT_RUN(test_120_authority_without_mirror_holds_before_carrier);
	UT_RUN(test_121_out_of_quorum_coordinator_cannot_submit_pgrd);
	UT_RUN(test_122_epoch_drift_rejects_pending_pgrd_without_mutation);
	UT_RUN(test_123_incarnation_drift_rejects_pending_pgrd_without_mutation);
	UT_RUN(test_124_cold_bootstrap_zero_historical_floor_accepts_live_pgrd_binding);
	UT_RUN(test_125_pgrd_snapshot_requires_majority_mirror_and_current_admission);
	UT_RUN(test_126_bit22_latch_fail_closed_without_shmem);
	UT_RUN(test_127_bit22_latch_defaults_inactive_then_apply_flips_and_records_round);
	UT_RUN(test_128_bit22_latch_second_apply_rejected_and_round_identity_kept);
	UT_RUN(test_128b_bit22_latch_same_round_apply_is_idempotent);
	UT_RUN(test_129_bit22_latch_rejects_zero_round_identity);
	UT_RUN(test_130_bit22_latch_apply_refused_while_census_red);
	UT_RUN(test_131_member_open_applied_applies_latch_and_acks);
	UT_RUN(test_132_member_open_applied_replay_is_idempotent);
	UT_RUN(test_133_member_open_applied_rejects_round_without_bit22);
	UT_RUN(test_134_member_open_applied_coordinator_does_not_apply);
	UT_RUN(test_135_member_open_applied_fail_closed_when_census_red);
	UT_RUN(test_136_coordinator_open_applied_advance_activates_and_publishes);
	UT_RUN(test_137_coordinator_open_applied_advance_waits_for_seam);
	UT_RUN(test_138_coordinator_open_applied_advance_fail_closed_on_activate_failure);
	UT_RUN(test_139_coordinator_open_applied_advance_rejects_mismatched_seam);
	UT_RUN(test_145_coordinator_latch_refused_leaves_round_commit_applied);
	UT_RUN(test_140_member_prepared_bit22_round_parameterized);
	UT_RUN(test_140a_member_retains_each_bit22_successor_and_fans_out_once);
	UT_RUN(test_140b_bit22_retained_successor_drift_invalidates_without_ack);
	UT_RUN(test_140c_bit22_prepared_request_waits_for_local_source_close);
	UT_RUN(test_140d_initial_r4_prepared_request_waits_for_local_source_close);
	UT_RUN(test_140e_initial_r4_member_fetches_pgrd_before_prepared_ack);
	UT_RUN(test_141_member_prepared_r4_round_keeps_four_member_shape);
	UT_RUN(test_142_cutover_begin_stages_seam_and_publishes_prepared);
	UT_RUN(test_143_cutover_begin_rejects_non_coordinator);
	UT_RUN(test_144_cutover_begin_fail_closed_on_create_failure);
	UT_RUN(test_145a_restore_open_proof_reconstructs_table);
	UT_RUN(test_145b_restore_open_proof_fail_closed_on_missing_open);
	UT_RUN(test_145c_restore_open_proof_fail_closed_on_root_refusal);
	UT_RUN(test_145d_restore_open_proof_fail_closed_on_membership_drift);
	UT_RUN(test_145e_restore_open_proof_is_idempotent);
	UT_RUN(test_145f_restore_open_proof_requires_active_latch);
	UT_RUN(test_145g_peer_open_matches_consumes_open_proof);
	UT_RUN(test_r4_peer_accepts_completed_ordinary_cutover_without_recovery_flag);
	UT_RUN(test_ordinary_open_reaches_real_master_route_and_stale_peer_does_not);
	UT_RUN(test_r4_peer_ordinary_open_rechecks_every_member_and_feature);
	UT_RUN(test_145h_resource_x_open_carrier_survives_unavailable_snapshot);
	UT_RUN(test_145i_resource_x_open_carrier_invalidates_formation_drift);
	UT_RUN(test_145j_resource_x_peer_match_reports_first_failed_predicate);
	UT_RUN(test_a89_peer_open_missing_and_torn_observations_are_not_identity_drift);
	UT_RUN(test_a89_peer_open_full_image_pending_and_independent_contradiction);
	UT_RUN(test_145k_restore_open_proof_does_not_overwrite_resource_x_carrier);
	UT_RUN(test_145l_epoch_zero_resource_x_open_carrier_survives_unavailable_snapshot);
	UT_RUN(test_145m_resource_x_prepare_carrier_survives_unavailable_snapshot);
	UT_RUN(test_145n_prepare_utility_has_real_pending_owner_after_projection);
	UT_RUN(test_145o_unavailable_membership_cannot_complete_prepared_utility);
	UT_RUN(test_145p_completed_prepare_cas_keeps_owner_before_local_projection);
	UT_RUN(test_145q_unavailable_observation_does_not_hide_identity_contradiction);
	UT_RUN(test_145r_each_mid_helper_membership_gap_preserves_same_owner);
	UT_RUN(test_145s_capability_gap_waits_but_other_identity_contradiction_wins);
	UT_RUN(test_145t_owned_commit_and_open_resume_without_a_new_utility);
	UT_RUN(test_145u_cas_binding_copy_contradiction_cannot_retain_utility);
	UT_RUN(test_a142_clean_restart_requests_fresh_open_while_closed);
	UT_RUN(test_a142_clean_start_observation_is_not_rearmable);
	UT_RUN(test_a142_unsafe_or_missing_startup_never_requests_open);
	UT_RUN(test_a142_wrong_durable_phase_member_or_old_boot_stays_closed);
	UT_RUN(test_a142_root_mismatch_cannot_emit_a_self_ack);
	UT_RUN(test_a142_missing_or_old_ack_never_opens_target);
	UT_RUN(test_a142_early_ack_waits_for_directed_request_and_local_root);
	UT_RUN(test_a142_actual_mailboxes_ack_fanin_and_native_proof_precede_open);
	UT_RUN(test_a142_early_ack_duplicate_is_idempotent_but_conflict_refuses);
	UT_RUN(test_a142_current_member_change_prevents_native_projection);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
