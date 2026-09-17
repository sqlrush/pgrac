/*-------------------------------------------------------------------------
 *
 * cluster_r4_activation_test_stubs.h
 *	  Process-lifecycle stubs for dependency-light semantic activation tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/cluster_r4_activation_test_stubs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_R4_ACTIVATION_TEST_STUBS_H
#define CLUSTER_R4_ACTIVATION_TEST_STUBS_H

#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "storage/ipc.h"
#include "access/xlog.h"

int MyProcPid = 101;
volatile sig_atomic_t InterruptPending = false;
volatile uint32 InterruptHoldoffCount = 0;
volatile uint32 QueryCancelHoldoffCount = 0;
volatile uint32 CritSectionCount = 0;
char *cluster_shared_data_dir;
static bool cluster_r4_activation_test_formation_valid;
static int32 cluster_r4_activation_test_membership_node = -1;
static uint64 cluster_r4_activation_test_membership_floor;
static ClusterMembershipState cluster_r4_activation_test_membership_state = CLUSTER_MEMBER_MEMBER;
static int32 cluster_r4_activation_test_membership_node2 = -1;
static uint64 cluster_r4_activation_test_membership_floor2;
static ClusterMembershipState cluster_r4_activation_test_membership_state2 = CLUSTER_MEMBER_MEMBER;
static uint64 cluster_r4_activation_test_self_incarnation = 1;
static uint64 cluster_r4_activation_test_current_epoch;
static bool cluster_r4_activation_test_in_quorum = true;
static bool cluster_r4_activation_test_admitted_snapshot_valid;
static uint64 cluster_r4_activation_test_admitted_members_lo;
static uint64 cluster_r4_activation_test_admitted_members_hi;
static uint64 cluster_r4_activation_test_admitted_epoch;
static uint32 cluster_r4_activation_test_admitted_snapshot_calls;

void ProcessInterrupts(void);

void
ProcessInterrupts(void)
{}

/* Dependency-light tests do not establish a clean startup. */
bool
cluster_qvotec_prior_unclean_death(void)
{
	return true;
}

uint64
cluster_epoch_get_current(void)
{
	return cluster_r4_activation_test_current_epoch;
}

bool
cluster_replacement_episode_is_valid(const ClusterReplacementEpisode *episode)
{
	return cluster_r4_activation_test_formation_valid && episode != NULL;
}

bool
cluster_reconfig_lmon_snapshot_replacement_admitted(ClusterReplacementEpisode *out_episode,
													ClusterReplacementCommitMarkerV3 *out_marker)
{
	if (!cluster_r4_activation_test_formation_valid || out_episode == NULL || out_marker == NULL)
		return false;

	memset(out_episode, 0, sizeof(*out_episode));
	out_episode->request_nonce = 1;
	out_episode->baseline_epoch = 1;
	out_episode->reserved_or_committed_epoch = 2;
	out_episode->old_admitted_incarnation = 1;
	out_episode->fresh_incarnation = 2;
	out_episode->grammar_fingerprint = 1;
	out_episode->target_node_id = 1;
	out_episode->coordinator_node_id = 0;
	out_episode->state_generation = 1;
	out_episode->phase = CLUSTER_REPLACEMENT_EPISODE_ADMITTED;
	out_episode->readiness_flags = CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK;

	memset(out_marker, 0, sizeof(*out_marker));
	out_marker->magic = CLUSTER_JCMK_MAGIC;
	out_marker->version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	out_marker->target_node_id = 1;
	out_marker->phase = CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED;
	out_marker->old_admitted_incarnation = 1;
	out_marker->fresh_incarnation = 2;
	out_marker->baseline_epoch = 1;
	out_marker->reserved_or_committed_epoch = 2;
	out_marker->request_nonce = 1;
	out_marker->grammar_fingerprint = 1;
	out_marker->ready_state_generation = 1;
	return true;
}

bool
cluster_qvotec_in_quorum(void)
{
	return cluster_r4_activation_test_in_quorum;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	return cluster_r4_activation_test_self_incarnation;
}

uint64
cluster_membership_get_last_admitted_incarnation(int32 node_id)
{
	if (node_id == cluster_r4_activation_test_membership_node)
		return cluster_r4_activation_test_membership_floor;
	if (node_id == cluster_r4_activation_test_membership_node2)
		return cluster_r4_activation_test_membership_floor2;
	return node_id >= 0 && node_id < CLUSTER_MAX_NODES ? 1 : 0;
}

ClusterMembershipState
cluster_membership_get_state(int32 node_id)
{
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_MEMBER_REMOVED;
	if (node_id == cluster_r4_activation_test_membership_node)
		return cluster_r4_activation_test_membership_state;
	if (node_id == cluster_r4_activation_test_membership_node2)
		return cluster_r4_activation_test_membership_state2;
	return CLUSTER_MEMBER_MEMBER;
}

bool
cluster_membership_is_member(int32 node_id)
{
	return cluster_membership_get_state(node_id) == CLUSTER_MEMBER_MEMBER;
}

uint64
GetSystemIdentifier(void)
{
	return UINT64_C(0x0123456789abcdef);
}

ClusterUndoSmgrRootMirrorState
cluster_undo_smgr_root_descriptor_read_candidate(
	const char *root_directory pg_attribute_unused(),
	uint8 observed[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] pg_attribute_unused())
{
	return CLUSTER_UNDO_SMGR_ROOT_MIRROR_ABSENT;
}

ClusterUndoSmgrRootMirrorState
cluster_undo_smgr_root_descriptor_publish(
	const char *root_directory pg_attribute_unused(),
	const uint8 image[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] pg_attribute_unused())
{
	return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
}

bool
cluster_reconfig_lmon_snapshot_admitted_membership(uint64 *out_members_lo, uint64 *out_members_hi,
												   uint64 *out_formation_epoch)
{
	cluster_r4_activation_test_admitted_snapshot_calls++;
	if (out_members_lo != NULL)
		*out_members_lo = 0;
	if (out_members_hi != NULL)
		*out_members_hi = 0;
	if (out_formation_epoch != NULL)
		*out_formation_epoch = 0;
	if (!cluster_r4_activation_test_admitted_snapshot_valid || out_members_lo == NULL
		|| out_members_hi == NULL || out_formation_epoch == NULL)
		return false;
	*out_members_lo = cluster_r4_activation_test_admitted_members_lo;
	*out_members_hi = cluster_r4_activation_test_admitted_members_hi;
	*out_formation_epoch = cluster_r4_activation_test_admitted_epoch;
	return true;
}

ClusterR4PrerequisiteSnapshot
cluster_reconfig_r4_prerequisite_snapshot(void)
{
	return (ClusterR4PrerequisiteSnapshot){
		.status = CLUSTER_R4_PREREQUISITE_RF_DEFERRED,
		.target_node_id = -1,
	};
}

bool
cluster_reconfig_r4_publish_ready(
	const ClusterR4PrerequisiteSnapshot *expected pg_attribute_unused())
{
	return false;
}

bool
cluster_undo_block0_current_startup_fenced_owned(void)
{
	return false;
}

ClusterLmsSharedState *
cluster_lms_shared_state(void)
{
	return NULL;
}

bool
cluster_lms_r4_drain_request(ClusterLmsSharedState *state pg_attribute_unused(),
							 uint64 generation pg_attribute_unused(),
							 uint64 *worker_incarnation pg_attribute_unused())
{
	return false;
}

void
cluster_lms_wakeup(int worker_id pg_attribute_unused())
{}

bool
cluster_cr_server_r4_lmon_reclaim_closed(uint64 worker_incarnation pg_attribute_unused(),
										 uint64 generation pg_attribute_unused())
{
	return false;
}

uint64
cluster_gcs_block_dedup_r4_route_purge_closed(void)
{
	return 0;
}

uint64
cluster_gcs_block_dedup_r4_route_count(void)
{
	return 0;
}

uint64
cluster_gcs_block_r4_requester_count(void)
{
	return 0;
}

bool cluster_sf_peer_capability_generation_matches(int32 peer_id, uint32 required_capabilities,
												   uint32 expected_generation);
static bool cluster_r4_activation_test_capability_generation_matches;
static int32 cluster_r4_activation_test_capability_peer = -1;
static uint64 cluster_r4_activation_test_capability_peer_mask;
static uint32 cluster_r4_activation_test_capability_required;
static uint32 cluster_r4_activation_test_capability_expected_generation;
bool
cluster_sf_peer_capability_generation_matches(int32 peer_id, uint32 required_capabilities,
											  uint32 expected_generation)
{
	return cluster_r4_activation_test_capability_generation_matches
		   && (peer_id == cluster_r4_activation_test_capability_peer
			   || (peer_id >= 0 && peer_id < 64
				   && (cluster_r4_activation_test_capability_peer_mask & (UINT64_C(1) << peer_id))
						  != 0))
		   && required_capabilities == cluster_r4_activation_test_capability_required
		   && expected_generation == cluster_r4_activation_test_capability_expected_generation;
}

static bool cluster_r4_activation_test_capability_word_sample_ok;
static uint32 cluster_r4_activation_test_capability_word;
static uint32 cluster_r4_activation_test_capability_generation;
static uint32 cluster_r4_activation_test_capability_sample_calls[CLUSTER_MAX_NODES];
static uint32 cluster_r4_activation_test_local_capability_word;
static ClusterICSendResult cluster_r4_activation_test_send_results[CLUSTER_MAX_NODES];
static uint32 cluster_r4_activation_test_send_calls[CLUSTER_MAX_NODES];
static uint8 cluster_r4_activation_test_send_payloads[CLUSTER_MAX_NODES]
													 [CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
static uint32 cluster_r4_activation_test_send_payload_lengths[CLUSTER_MAX_NODES];
static uint8 cluster_r4_activation_test_send_msg_types[CLUSTER_MAX_NODES];
static uint32 cluster_r4_activation_test_close_calls[CLUSTER_MAX_NODES];

/* Same fixture record as word_sample, including an unavailable observation.
 * It never supplies a positive proof when the fixture record is invalid. */
bool
cluster_sf_peer_capability_record_snapshot(int32 peer_id, ClusterSfPeerCap *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return false;
	cluster_r4_activation_test_capability_sample_calls[peer_id]++;
	out->valid = cluster_r4_activation_test_capability_word_sample_ok;
	out->bits = cluster_r4_activation_test_capability_word;
	out->generation = cluster_r4_activation_test_capability_generation;
	return true;
}

bool
cluster_sf_peer_capability_word_sample(int32 peer_id, uint32 required_capabilities,
									   uint32 *capability_word_out, uint32 *generation_out)
{
	if (capability_word_out != NULL)
		*capability_word_out = 0;
	if (generation_out != NULL)
		*generation_out = 0;
	if (peer_id >= 0 && peer_id < CLUSTER_MAX_NODES)
		cluster_r4_activation_test_capability_sample_calls[peer_id]++;
	if (!cluster_r4_activation_test_capability_word_sample_ok || peer_id < 0
		|| peer_id >= CLUSTER_MAX_NODES || required_capabilities == 0
		|| (cluster_r4_activation_test_capability_word & required_capabilities)
			   != required_capabilities)
		return false;
	if (capability_word_out != NULL)
		*capability_word_out = cluster_r4_activation_test_capability_word;
	if (generation_out != NULL)
		*generation_out = cluster_r4_activation_test_capability_generation;
	return true;
}

uint32
cluster_ic_local_capability_word(void)
{
	return cluster_r4_activation_test_local_capability_word;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id, const void *payload,
						 uint32 payload_len)
{
	if (dest_node_id < 0 || dest_node_id >= CLUSTER_MAX_NODES || payload == NULL
		|| payload_len != CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES)
		return CLUSTER_IC_SEND_HARD_ERROR;
	cluster_r4_activation_test_send_calls[dest_node_id]++;
	cluster_r4_activation_test_send_msg_types[dest_node_id] = msg_type;
	cluster_r4_activation_test_send_payload_lengths[dest_node_id] = payload_len;
	memcpy(cluster_r4_activation_test_send_payloads[dest_node_id], payload, payload_len);
	return cluster_r4_activation_test_send_results[dest_node_id];
}

void
cluster_ic_tier1_close_peer(int32 peer_id, const char *reason pg_attribute_unused())
{
	if (peer_id >= 0 && peer_id < CLUSTER_MAX_NODES)
		cluster_r4_activation_test_close_calls[peer_id]++;
}

void
on_shmem_exit(pg_on_exit_callback function pg_attribute_unused(), Datum arg pg_attribute_unused())
{}

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
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

#endif /* CLUSTER_R4_ACTIVATION_TEST_STUBS_H */
