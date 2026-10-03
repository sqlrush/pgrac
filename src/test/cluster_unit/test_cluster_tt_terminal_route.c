/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_terminal_route.c
 *    Execute the original verdict resolver and membership route wrappers.
 *
 * The remote proof boundary is counted, not replaced with a visibility rule.
 * These tests require the terminal request to reach that boundary without
 * reclassifying a stale heartbeat as a live owner or borrowing survivor proof.
 * The production admission/session provider must qualify any new request leg;
 * successful fixture replies are not evidence that such a provider exists.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cr.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_undo_authority.h"
#include "cluster/cluster_undo_gcs.h"
#include "cluster/cluster_undo_horizon.h"
#include "cluster/cluster_undo_resid.h"
#include "cluster/cluster_undo_segment.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define TEST_XID ((TransactionId)4202129)
#define TEST_SCN ((SCN)72057594041353497ULL)

int cluster_node_id = 2;
bool cluster_enabled = true;
bool cluster_crossnode_runtime_visibility = true;
bool cluster_undo_gcs_coherence = true;

static ClusterMembershipState member_state[CLUSTER_MAX_NODES];
static bool member_fresh[CLUSTER_MAX_NODES];
static uint64 test_epoch;
static bool read_admitted;
static ClusterConf test_conf;
ClusterConf *ClusterConfShmem = &test_conf;
static bool pair_proven;
static bool ordinary_live;
static int ordinary_calls;
static int pair_calls;
static int route_failures;
static int resolve_failures;
static int enter_calls, capture_calls, current_calls, leave_calls;
static bool target_admitted, capture_ok, current_ok, token_held;
static bool throw_fetch, bump_epoch, remove_owner, restore_freshness;
static bool recovering, cssd_dead, covers_ok;
static uint8 reply_kind;
static SCN reply_scn;
static ClusterSemanticTerminalPeerSnapshot captured;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT_EQ(feature, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(side, CLUSTER_SEMANTIC_TARGET_SIDE);
	enter_calls++;
	memset(token, 0, sizeof(*token));
	if (!target_admitted)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	token->feature_bit = feature;
	token->side = side;
	token->record_generation = 7;
	token->formation_epoch = test_epoch;
	token->entered = token_held = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_terminal_peer_capture(const ClusterSemanticAdmissionToken *token,
												  int32 peer, uint32 caps,
												  ClusterSemanticTerminalPeerSnapshot *out)
{
	UT_ASSERT(token_held && token->entered);
	UT_ASSERT_EQ(peer, 1);
	UT_ASSERT_EQ(caps, PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1
						   | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1
						   | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1);
	capture_calls++;
	memset(out, 0, sizeof(*out));
	if (!capture_ok)
		return false;
	out->record_generation = token->record_generation;
	out->formation_epoch = token->formation_epoch;
	out->peer_node_id = peer;
	out->peer_boot_id = 191;
	out->peer_data_generation[0] = 41;
	out->membership_cut_generation = 11;
	captured = *out;
	return true;
}

bool
cluster_semantic_activation_terminal_peer_current(
	const ClusterSemanticAdmissionToken *token, const ClusterSemanticTerminalPeerSnapshot *expected)
{
	UT_ASSERT(token_held && token->entered);
	UT_ASSERT_EQ(memcmp(expected, &captured, sizeof(*expected)), 0);
	UT_ASSERT_EQ(pair_calls, 1);
	current_calls++;
	return current_ok;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT(token_held && token->entered);
	leave_calls++;
	token_held = false;
	memset(token, 0, sizeof(*token));
}

ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 peer)
{
	return cssd_dead ? CLUSTER_CSSD_PEER_DEAD : CLUSTER_CSSD_PEER_ALIVE;
}
bool
cluster_grd_recovery_in_progress(void)
{
	return recovering;
}
bool
cluster_vis_live_authority_covers(SCN demand, ClusterLiveAuthority auth)
{
	return cluster_vis_live_authority_covers_policy(demand, auth, test_epoch);
}
void
cluster_rtvis_verdict_note_failclosed(void)
{}
void
cluster_rtvis_verdict_note_wire(void)
{}
void
cluster_rtvis_verdict_note_exact(void)
{}
void
cluster_rtvis_verdict_note_below_horizon(void)
{}
void
cluster_rtvis_verdict_note_inadmissible(void)
{}
void
cluster_vis_bump_covers_scn_refuse_count(void)
{}
void
cluster_vis53r97_note_covers_refuse(void)
{}

uint64
cluster_epoch_get_current(void)
{
	return test_epoch;
}
bool
cluster_undo_horizon_read_admission_enforce(SCN read_scn)
{
	return read_admitted;
}
ClusterMembershipState
cluster_membership_get_state(int32 node)
{
	return member_state[node];
}
bool
cluster_reconfig_get_observed_fresh_alive(int32 node)
{
	return member_fresh[node];
}
void
cluster_undo_authority_note_failclosed(void)
{
	route_failures++;
}
void
cluster_rtvis_resolve_note_failclosed(void)
{
	resolve_failures++;
}
void
cluster_rtvis_resolve_note_committed(void)
{
	abort();
}
void
cluster_rtvis_resolve_note_aborted(void)
{
	abort();
}
void
cluster_scn_observe(SCN scn)
{}
bool
cluster_peer_supports_undo_authority_serve(int32 peer)
{
	abort();
}
bool
cluster_gcs_block_undo_authority_verdict_fetch_and_wait(int32 peer, int32 origin, uint32 segment,
														TransactionId xid,
														ClusterUndoVerdictResult *result)
{
	abort();
}

/* Encoding is not under test; retain the exact owner passed by the resolver. */
void
cluster_undo_resid_encode(int32 owner, uint32 segment, uint32 block, uint32 generation,
						  ClusterResId *out)
{
	memset(out, 0, sizeof(*out));
	out->field4 = (uint16)owner;
}
int32
cluster_undo_resid_master(const ClusterResId *resid)
{
	return resid->field4;
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}

/* Native SCN time ordering; unrelated policy functions share this object. */
int
scn_time_cmp(SCN left, SCN right)
{
	uint64 a = left & SCN_LOCAL_MASK;
	uint64 b = right & SCN_LOCAL_MASK;

	return (a > b) - (a < b);
}

static inline void
bm_set(uint8 *bitmap, int node)
{
	bitmap[node >> 3] |= (uint8)(1u << (node & 7));
}

static ClusterUndoVerdictResult
rtvis_resolve_self_verdict(TransactionId xid, uint32 segment, uint32 slot, SCN read_scn,
						   bool authoritative, SCN pair)
{
	abort();
}

static ClusterUndoVerdictResult
rtvis_authority_serve_block0(int origin, uint32 segment, TransactionId xid,
							 const ClusterUndoServeRoute *route)
{
	abort();
}

static bool
rtvis_try_resolve_remote_internal(int origin, uint32 segment, uint32 slot, TransactionId xid,
								  uint32 epoch, SCN pair, SCN read_scn, bool authoritative,
								  bool *committed, bool *in_progress, SCN *scn, bool *bound)
{
	/* A stale route must never reach the coherent block0 fetch wrapper. */
	UT_ASSERT(member_fresh[origin]);
	UT_ASSERT_EQ(origin, 1);
	UT_ASSERT_EQ(segment, 257);
	UT_ASSERT_EQ(slot, 2);
	UT_ASSERT_EQ(xid, TEST_XID);
	UT_ASSERT(authoritative);
	*committed = false;
	*in_progress = false;
	*scn = InvalidScn;
	*bound = false;
	if (!SCN_VALID(pair)) {
		ordinary_calls++;
		*in_progress = ordinary_live;
		return ordinary_live;
	}
	pair_calls++;
	if (!pair_proven)
		return false;
	*committed = true;
	*scn = pair;
	return true;
}

bool
cluster_gcs_block_undo_freshref_c1b_pair_fetch_and_wait(int32 origin, uint32 segment, uint32 slot,
														TransactionId xid, uint32 epoch, SCN pair,
														ClusterGcsUndoVerdictPage *verdict,
														ClusterLiveAuthority *auth)
{
	UT_ASSERT(token_held);
	UT_ASSERT_EQ(capture_calls, 1);
	UT_ASSERT_EQ(current_calls, 0);
	UT_ASSERT_EQ(origin, 1);
	UT_ASSERT_EQ(segment, 257);
	UT_ASSERT_EQ(slot, 2);
	UT_ASSERT_EQ(xid, TEST_XID);
	UT_ASSERT_EQ(epoch, test_epoch);
	UT_ASSERT_EQ(pair, TEST_SCN);
	pair_calls++;
	if (throw_fetch)
		pg_re_throw();
	memset(verdict, 0, sizeof(*verdict));
	verdict->verdict = reply_kind;
	verdict->commit_scn = reply_scn;
	memset(auth, 0, sizeof(*auth));
	auth->origin_epoch = test_epoch;
	auth->live_hwm_lsn = 128;
	auth->authority_scn = covers_ok ? TEST_SCN : InvalidScn;
	if (bump_epoch)
		test_epoch++;
	if (remove_owner)
		member_state[1] = CLUSTER_MEMBER_REMOVED;
	if (restore_freshness)
		member_fresh[1] = true;
	return pair_proven;
}
bool
cluster_gcs_block_undo_verdict_fetch_and_wait(int32 origin, uint32 segment, uint32 slot,
											  TransactionId xid, bool authoritative,
											  ClusterGcsUndoVerdictPage *verdict,
											  ClusterLiveAuthority *auth)
{
	abort();
}

/* Debug output is unrelated to the returned proof and release semantics. */
#undef elog
#define elog(...) ((void)0)
#include "test_cluster_tt_terminal_route.inc"

static void
reset_fixture(void)
{
	int node;

	memset(member_state, 0, sizeof(member_state));
	memset(member_fresh, 0, sizeof(member_fresh));
	for (node = 0; node < 4; node++) {
		member_state[node] = CLUSTER_MEMBER_MEMBER;
		member_fresh[node] = node != 1;
	}
	test_epoch = 0;
	read_admitted = true;
	test_conf.node_count = 4;
	pair_proven = true;
	ordinary_live = false;
	ordinary_calls = pair_calls = route_failures = resolve_failures = 0;
	enter_calls = capture_calls = current_calls = leave_calls = 0;
	target_admitted = capture_ok = current_ok = covers_ok = true;
	token_held = throw_fetch = bump_epoch = remove_owner = restore_freshness = false;
	recovering = cssd_dead = false;
	reply_kind = CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT;
	reply_scn = TEST_SCN;
}

static ClusterUndoVerdictResult
sample(bool terminal_pair)
{
	return cluster_undo_verdict_resolve_internal(1, 257, TEST_XID, 2, (SCN)72057594041400000ULL,
												 true, TEST_XID, 0,
												 terminal_pair ? TEST_SCN : InvalidScn);
}

UT_TEST(stale_member_route_is_still_unknown)
{
	int32 authority = 99;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_serve_authority_lookup(1, 0, &authority),
				 CLUSTER_UNDO_AUTHORITY_UNKNOWN);
	UT_ASSERT_EQ(authority, -1);
}

UT_TEST(stale_member_terminal_request_can_obtain_existing_proof)
{
	ClusterUndoVerdictResult result;

	reset_fixture();
	result = sample(true);
	UT_ASSERT_EQ(pair_calls, 1);
	UT_ASSERT_EQ(ordinary_calls, 0);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(result.commit_scn, TEST_SCN);
}

UT_TEST(stale_member_failed_proof_remains_unknown)
{
	ClusterUndoVerdictResult result;

	reset_fixture();
	pair_proven = false;
	result = sample(true);
	UT_ASSERT_EQ(pair_calls, 1);
	UT_ASSERT_EQ(ordinary_calls, 0);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(result.commit_scn, InvalidScn);
}

UT_TEST(stale_member_without_terminal_pair_keeps_original_gate)
{
	ClusterUndoVerdictResult result;

	reset_fixture();
	result = sample(false);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(ordinary_calls + pair_calls, 0);
	UT_ASSERT_EQ(route_failures, 1);
}

UT_TEST(read_admission_denied_sends_nothing)
{
	reset_fixture();
	read_admitted = false;
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(ordinary_calls + pair_calls, 0);
}

UT_TEST(stale_reference_epoch_sends_nothing)
{
	reset_fixture();
	test_epoch = 1;
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(ordinary_calls + pair_calls, 0);
}

UT_TEST(dead_owner_does_not_borrow_survivor_terminal_proof)
{
	reset_fixture();
	member_state[1] = CLUSTER_MEMBER_DEAD;
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(ordinary_calls + pair_calls, 0);
}

UT_TEST(nonmember_does_not_gain_terminal_request_permission)
{
	ClusterMembershipState denied[] = { CLUSTER_MEMBER_REMOVED, CLUSTER_MEMBER_JOINING,
										CLUSTER_MEMBER_ABSENT, CLUSTER_MEMBER_REJECTED };
	size_t i;

	for (i = 0; i < lengthof(denied); i++) {
		reset_fixture();
		member_state[1] = denied[i];
		UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
		UT_ASSERT_EQ(ordinary_calls + pair_calls, 0);
	}
}

UT_TEST(fresh_owner_preserves_exact_live_first)
{
	reset_fixture();
	member_fresh[1] = true;
	ordinary_live = true;
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_IN_PROGRESS);
	UT_ASSERT_EQ(ordinary_calls, 1);
	UT_ASSERT_EQ(pair_calls, 0);
}

UT_TEST(fresh_owner_ordinary_miss_reaches_existing_pair)
{
	reset_fixture();
	member_fresh[1] = true;
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(ordinary_calls, 1);
	UT_ASSERT_EQ(pair_calls, 1);
}


UT_TEST(terminal_capture_refusal_releases_admission)
{
	reset_fixture();
	capture_ok = false;
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(capture_calls, 1);
	UT_ASSERT_EQ(pair_calls, 0);
	UT_ASSERT_EQ(leave_calls, 1);
	UT_ASSERT(!token_held);
}
UT_TEST(terminal_target_admission_refusal_sends_nothing)
{
	reset_fixture();
	target_admitted = false;
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(enter_calls, 1);
	UT_ASSERT_EQ(capture_calls, 0);
	UT_ASSERT_EQ(pair_calls, 0);
	UT_ASSERT_EQ(leave_calls, 0);
}
UT_TEST(terminal_original_identity_is_rechecked_before_leave)
{
	reset_fixture();
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(current_calls, 1);
	UT_ASSERT_EQ(leave_calls, 1);
	UT_ASSERT(!token_held);
}
UT_TEST(terminal_peer_session_or_cut_change_discards_proof)
{
	reset_fixture();
	current_ok = false;
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(pair_calls, 1);
	UT_ASSERT_EQ(current_calls, 1);
	UT_ASSERT_EQ(leave_calls, 1);
	UT_ASSERT(!token_held);
}
UT_TEST(terminal_epoch_change_discards_proof)
{
	reset_fixture();
	bump_epoch = true;
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(pair_calls, 1);
	UT_ASSERT_EQ(leave_calls, 1);
}
UT_TEST(terminal_removed_owner_discards_proof)
{
	reset_fixture();
	remove_owner = true;
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(pair_calls, 1);
	UT_ASSERT_EQ(leave_calls, 1);
}
UT_TEST(terminal_freshness_restored_does_not_invalidate_exact_proof)
{
	reset_fixture();
	restore_freshness = true;
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(pair_calls, 1);
	UT_ASSERT_EQ(ordinary_calls, 0);
	UT_ASSERT_EQ(current_calls, 1);
	UT_ASSERT_EQ(leave_calls, 1);
}
UT_TEST(terminal_nonzero_epoch_uses_same_identity_bracket)
{
	ClusterUndoVerdictResult result;
	reset_fixture();
	test_epoch = 3;
	result = cluster_undo_verdict_resolve_internal(1, 257, TEST_XID, 2, TEST_SCN, true, TEST_XID, 3,
												   TEST_SCN);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(current_calls, 1);
	UT_ASSERT_EQ(leave_calls, 1);
}
UT_TEST(terminal_wrong_scn_and_nonterminal_kinds_refused)
{
	uint8 kinds[] = { CLUSTER_GCS_UNDO_VERDICT_ABORTED, CLUSTER_GCS_UNDO_VERDICT_IN_PROGRESS,
					  CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON,
					  CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT };
	size_t i;
	for (i = 0; i < lengthof(kinds); i++) {
		reset_fixture();
		reply_kind = kinds[i];
		reply_scn = TEST_SCN + 1;
		UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
		UT_ASSERT_EQ(pair_calls, 1);
		UT_ASSERT_EQ(leave_calls, 1);
	}
}
UT_TEST(terminal_missing_authority_covers_refused)
{
	reset_fixture();
	covers_ok = false;
	UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(pair_calls, 1);
	UT_ASSERT_EQ(leave_calls, 1);
}
UT_TEST(terminal_existing_recovery_and_cssd_dead_gate_remains)
{
	int i;
	for (i = 0; i < 2; i++) {
		reset_fixture();
		recovering = i == 0;
		cssd_dead = i == 1;
		UT_ASSERT_EQ(sample(true).kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
		UT_ASSERT_EQ(capture_calls, 1);
		UT_ASSERT_EQ(pair_calls, 0);
		UT_ASSERT_EQ(leave_calls, 1);
	}
}
UT_TEST(terminal_transport_error_releases_admission_and_rethrows)
{
	volatile bool caught = false;
	reset_fixture();
	throw_fetch = true;
	PG_TRY();
	{
		(void)sample(true);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(leave_calls, 1);
	UT_ASSERT(!token_held);
	UT_ASSERT_EQ(current_calls, 0);
}

int
main(void)
{
	UT_PLAN(22);
	UT_RUN(stale_member_route_is_still_unknown);
	UT_RUN(stale_member_terminal_request_can_obtain_existing_proof);
	UT_RUN(stale_member_failed_proof_remains_unknown);
	UT_RUN(stale_member_without_terminal_pair_keeps_original_gate);
	UT_RUN(read_admission_denied_sends_nothing);
	UT_RUN(stale_reference_epoch_sends_nothing);
	UT_RUN(dead_owner_does_not_borrow_survivor_terminal_proof);
	UT_RUN(nonmember_does_not_gain_terminal_request_permission);
	UT_RUN(fresh_owner_preserves_exact_live_first);
	UT_RUN(fresh_owner_ordinary_miss_reaches_existing_pair);
	UT_RUN(terminal_capture_refusal_releases_admission);
	UT_RUN(terminal_target_admission_refusal_sends_nothing);
	UT_RUN(terminal_original_identity_is_rechecked_before_leave);
	UT_RUN(terminal_peer_session_or_cut_change_discards_proof);
	UT_RUN(terminal_epoch_change_discards_proof);
	UT_RUN(terminal_removed_owner_discards_proof);
	UT_RUN(terminal_freshness_restored_does_not_invalidate_exact_proof);
	UT_RUN(terminal_nonzero_epoch_uses_same_identity_bracket);
	UT_RUN(terminal_wrong_scn_and_nonterminal_kinds_refused);
	UT_RUN(terminal_missing_authority_covers_refused);
	UT_RUN(terminal_existing_recovery_and_cssd_dead_gate_remains);
	UT_RUN(terminal_transport_error_releases_admission_and_rethrows);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
