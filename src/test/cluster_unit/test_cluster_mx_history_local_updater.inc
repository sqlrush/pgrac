/* Real collector/graph/retention and local B consumer. The original runtime
 * provenance API is an external authority boundary here, exercised separately
 * by test_cluster_r4_tx_outcome; this fixture does not prove native DATA/TT. */
#include "cluster/cluster_runtime_visibility.h"
static unsigned local_updater_calls, local_updater_fault;
static bool local_updater_enabled;
bool
cluster_runtime_visibility_current_mx_updater_provenance_exact(const ClusterTxLocator *locator,
	TimestampTz deadline, ClusterTTStatusKey *key, ClusterTTStatusResult *result, uint32 *grant,
	uint32 *cap, ClusterCtrcTxnKeyV1 *ctrc, ClusterTxLocator *canonical, bool *cross, bool *retry)
{
	if (!local_updater_enabled) abort(); /* Legacy A sampling stays forbidden. */
	local_updater_calls++;
	UT_ASSERT(!content_locked && CritSectionCount == 0);
	UT_ASSERT_EQ(remembered, 2); UT_ASSERT_EQ(finish_reads, 2); /* A first. */
	UT_ASSERT_EQ(updater_fetches, 0); UT_ASSERT_EQ(deadline, 0);
	UT_ASSERT_EQ(locator->xid, 96); UT_ASSERT_EQ(locator->tt_wrap, TT_WRAP_INVALID);
	UT_ASSERT_EQ(uba_origin_node_id(locator->uba), cluster_node_id);
	memset(key, 0, sizeof(*key)); memset(result, 0, sizeof(*result));
	memset(ctrc, 0, sizeof(*ctrc)); memset(canonical, 0, sizeof(*canonical));
	*grant = *cap = 0; *cross = *retry = false;
	if (local_updater_fault == 1) return false;
	if (local_updater_fault == 2) { *retry = true; return false; }
	if (local_updater_fault == 3) pg_re_throw();
	*canonical = *locator; canonical->tt_wrap = 0;
	key->local_xid = 96; result->authoritative = true;
	result->status = CLUSTER_TT_STATUS_COMMITTED; result->commit_scn = 90;
	if (local_updater_fault == 4) canonical->xid++;
	if (local_updater_fault == 5) canonical->tt_wrap = TT_WRAP_INVALID;
	if (local_updater_fault == 6) canonical->uba = uba_encode(1, 10, 3, 0);
	if (local_updater_fault == 7) pg_atomic_write_u32(&history_gate.transition_closed, 1);
	if (local_updater_fault == 8) CurrentResourceOwner = (ResourceOwner)2;
	return true;
}
static Snapshot
local_updater_setup(ClusterCurrentUpdaterChallenge *challenge)
{
	Snapshot s = updater_remote_setup(challenge);
	cluster_node_id = 0; local_updater_calls = local_updater_fault = 0;
	local_updater_enabled = true;
	return s;
}
static void
local_updater_clean(ClusterMxHistoryCollector *c, Snapshot s)
{
	clean(c, s); local_updater_enabled = false; cluster_node_id = 1;
	members[0] = (ClusterCurrentMxMemberDesc){ 101, MultiXactStatusForShare, { 0 } };
	members[1].xid = 102;
}
static void
local_updater_delivers_independent_B_once(void)
{
	ClusterCurrentUpdaterChallenge challenge; ClusterCurrentUpdaterProof b;
	Snapshot s = local_updater_setup(&challenge); ClusterMxHistoryCollector *c = NULL;
	UT_ASSERT(updater_begin(s, &challenge, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(cluster_mx_history_poll(c), CMXH_COMPLETE);
	UT_ASSERT_EQ(local_updater_calls, 1); UT_ASSERT_EQ(updater_fetches, 0);
	UT_ASSERT_EQ(updater_deliver(c, &challenge, &b), CMXH_COMPLETE);
	UT_ASSERT_EQ(b.verdict, CUCP_MATCH); UT_ASSERT_EQ(b.updater_xid, 96);
	UT_ASSERT_EQ(b.candidate_next_xmin_locator.tt_wrap, 0);
	UT_ASSERT_EQ(memcmp(&b.candidate_next_xmin_alias, &challenge.candidate_next_xmin_alias,
		sizeof(b.candidate_next_xmin_alias)), 0);
	UT_ASSERT_EQ(finish_proofs[0].base.member_xid, 96);
	UT_ASSERT_EQ(finish_proofs[0].effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT_EQ(finish_proofs[1].effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	UT_ASSERT_EQ(finish_reads, 2); UT_ASSERT_EQ(remembered, 2);
	UT_ASSERT_EQ(updater_deliver(c, &challenge, &b), CMXH_UNKNOWN);
	UT_ASSERT_EQ(b.verdict, CUCP_UNKNOWN); finish_unknown(2);
	local_updater_clean(c, s);
}
static void
local_updater_failure_invalidates_A_and_B(void)
{
	for (unsigned fault = 1; fault <= 6; ++fault) {
		ClusterCurrentUpdaterChallenge challenge; ClusterCurrentUpdaterProof b;
		Snapshot s; ClusterMxHistoryCollector *c = NULL;
		if (fault == 2 || fault == 3) continue;
		s = local_updater_setup(&challenge); local_updater_fault = fault;
		UT_ASSERT(updater_begin(s, &challenge, &c));
		UT_ASSERT_EQ(finish_ready(c, false), CMXH_UNKNOWN);
		UT_ASSERT_EQ(local_updater_calls, 1); UT_ASSERT_EQ(finish_reads, 2);
		UT_ASSERT_EQ(remembered, 1);
		UT_ASSERT_EQ(updater_deliver(c, &challenge, &b), CMXH_UNKNOWN);
		UT_ASSERT_EQ(b.verdict, CUCP_UNKNOWN); finish_unknown(2);
		local_updater_clean(c, s);
	}
}
static void
local_updater_precommit_requalifies_whole_attempt(void)
{
	ClusterCurrentUpdaterChallenge challenge; ClusterCurrentUpdaterProof b;
	Snapshot s = local_updater_setup(&challenge); ClusterMxHistoryCollector *c = NULL;
	local_updater_fault = 2;
	UT_ASSERT(updater_begin(s, &challenge, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_REQUALIFY);
	UT_ASSERT_EQ(local_updater_calls, 1); UT_ASSERT_EQ(remembered, 1);
	UT_ASSERT_EQ(updater_deliver(c, &challenge, &b), CMXH_UNKNOWN); finish_unknown(2);
	/* A/B cannot survive a precommit observation; a fresh attempt may complete. */
	cluster_mx_history_cancel(c); c = NULL; local_updater_fault = 0;
	finish_sources(); finish_top = 80;
	for (unsigned i = 0; i < 2; ++i) ClusterPageGetItlSlots(BufferBlocks)[i].xid = 80;
	UT_ASSERT(updater_begin(s, &challenge, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(updater_deliver(c, &challenge, &b), CMXH_COMPLETE);
	UT_ASSERT_EQ(local_updater_calls, 2);
	local_updater_clean(c, s);
}
static void
local_updater_scope_drift_discards_both(void)
{
	for (unsigned fault = 7; fault <= 8; ++fault) {
		ClusterCurrentUpdaterChallenge challenge; ClusterCurrentUpdaterProof b;
		Snapshot s = local_updater_setup(&challenge); ClusterMxHistoryCollector *c = NULL;
		local_updater_fault = fault;
		UT_ASSERT(updater_begin(s, &challenge, &c));
		UT_ASSERT_EQ(finish_ready(c, false), CMXH_UNKNOWN);
		UT_ASSERT_EQ(local_updater_calls, 1); UT_ASSERT_EQ(remembered, 1);
		UT_ASSERT_EQ(updater_deliver(c, &challenge, &b), CMXH_UNKNOWN); finish_unknown(2);
		local_updater_clean(c, s);
	}
}
static void
local_updater_error_releases_attempt(void)
{
	ClusterCurrentUpdaterChallenge challenge; ClusterCurrentUpdaterProof b;
	Snapshot s = local_updater_setup(&challenge); ClusterMxHistoryCollector *c = NULL;
	volatile bool caught = false;
	local_updater_fault = 3; UT_ASSERT(updater_begin(s, &challenge, &c));
	PG_TRY(); { (void)finish_ready(c, false); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(local_updater_calls, 1); UT_ASSERT_EQ(remembered, 1);
	UT_ASSERT_EQ(updater_deliver(c, &challenge, &b), CMXH_UNKNOWN); finish_unknown(2);
	local_updater_clean(c, s);
}
static void
local_updater_finish_still_checks_fresh_challenge(void)
{
	ClusterCurrentUpdaterChallenge challenge; ClusterCurrentUpdaterProof b;
	Snapshot s = local_updater_setup(&challenge); ClusterMxHistoryCollector *c = NULL;
	UT_ASSERT(updater_begin(s, &challenge, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	challenge.candidate_next_xmin_locator.itl_slot_index++;
	UT_ASSERT_EQ(updater_deliver(c, &challenge, &b), CMXH_REQUALIFY);
	UT_ASSERT_EQ(b.verdict, CUCP_UNKNOWN); finish_unknown(2);
	UT_ASSERT_EQ(local_updater_calls, 1); UT_ASSERT_EQ(remembered, 1);
	local_updater_clean(c, s);
}
