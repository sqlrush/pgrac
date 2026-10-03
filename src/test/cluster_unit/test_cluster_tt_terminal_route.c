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
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_undo_authority.h"
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
{
	abort();
}
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

int
main(void)
{
	UT_PLAN(10);
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
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
