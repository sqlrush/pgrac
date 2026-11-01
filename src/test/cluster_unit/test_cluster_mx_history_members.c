/* Real graph/codec/registered landing. Remote DATA, canonical outcomes and
 * grant numbers are controlled wire inputs, not native TT/CTRC witnesses.
 * Portions Copyright (c) 2026, pgrac contributors */
int members_unused_graph_main(void);
#define HISTORY_GRAPH_TEST_MAIN members_unused_graph_main
#include "test_cluster_mx_history_graph.c"
#include "cluster/cluster_mx_history_member.h"

static ClusterSavepointOperationKey member_keys[CLUSTER_CURRENT_MX_MAX_MEMBERS];
static ClusterCurrentMemberOperationProof member_proofs[CLUSTER_CURRENT_MX_MAX_MEMBERS];

static void
member_descriptor(void)
{
	input.descriptor_hash
		= cluster_multixact_current_descriptor_hash(&input.mxkey, input.members, input.nmembers);
}

static void
member_chain(size_t n)
{
	graph_reset(n);
	for (size_t i = 0; i < n; ++i)
		sources[i] = source_at(n + 7 - i);
}

static ClusterMxHistoryGraph *
member_begin(size_t n, bool order)
{
	ClusterMxHistoryGraph *g;
	for (size_t i = n; i > 1; --i) {
		sources[i - 2].previous = sources[i - 1].address;
		if (order)
			sources[i - 2].prior = source_head(&sources[i - 1]);
	}
	input.roots[0] = source_head(&sources[0]);
	member_descriptor();
	g = graph_begin();
	UT_ASSERT(g != NULL);
	UT_ASSERT_EQ(graph_run(g), CMXH_COMPLETE);
	memset(member_keys, 0xa5, sizeof(member_keys));
	memset(member_proofs, 0xa5, sizeof(member_proofs));
	return g;
}

static bool
member_assemble(ClusterMxHistoryGraph *g)
{
	unsigned fetched = graph_fetches;
	bool ok = cluster_mx_history_assemble_members(&input, g, member_keys, member_proofs,
												  input.nmembers);
	UT_ASSERT_EQ(graph_fetches, fetched);
	return ok;
}

static void
member_unknown(uint16 n)
{
	ClusterSavepointOperationKey zero = { 0 };
	for (uint16 i = 0; i < n; ++i) {
		UT_ASSERT_EQ(member_proofs[i].base.state, CCM_UNKNOWN);
		UT_ASSERT_EQ(member_proofs[i].format_version, 2);
		UT_ASSERT_EQ(memcmp(&member_keys[i], &zero, sizeof(zero)), 0);
	}
}

UT_TEST(members_original_order_and_disposition)
{
	ClusterMxHistoryGraph *g;
	member_chain(2);
	sources[0].child = 104;
	sources[0].effect = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
	g = member_begin(2, true);
	UT_ASSERT(member_assemble(g));
	for (uint16 i = 0; i < 2; ++i) {
		const ClusterCurrentMemberOperationProof *p = &member_proofs[i];
		UT_ASSERT_EQ(p->base.member_xid, input.members[i].xid);
		UT_ASSERT_EQ(p->base.member_ordinal, i);
		UT_ASSERT_EQ(p->base.member_status, input.members[i].member_status);
		UT_ASSERT_EQ(p->base.state, CCM_COMMITTED);
		UT_ASSERT_EQ(p->base.key.local_xid, 100);
		UT_ASSERT_EQ(p->base.key.segment_generation, 12);
		UT_ASSERT_EQ(p->base.key.slot_wrap, 2);
		UT_ASSERT_EQ(p->base.commit_scn, 1100);
		UT_ASSERT_EQ(p->flags, CLUSTER_CURRENT_OPERATION_PROOF_EXACT);
		UT_ASSERT(FullTransactionIdEquals(
			p->operation_fxid, FullTransactionIdFromEpochAndXid(2, input.members[i].xid)));
	}
	UT_ASSERT_EQ(member_proofs[0].effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT_EQ(member_proofs[1].effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	UT_ASSERT(cluster_undo_record_uba_equal(member_keys[0].locator.uba, sources[1].address));
	graph_clean(g);
}

UT_TEST(members_newest_uses_page_prior_not_lsn)
{
	ClusterMxHistoryGraph *g;
	member_chain(3);
	input.nmembers = 1;
	sources[1].lsn = 999999;
	g = member_begin(3, true);
	UT_ASSERT(member_assemble(g));
	UT_ASSERT(cluster_undo_record_uba_equal(member_keys[0].locator.uba, sources[0].address));
	graph_clean(g);
}

UT_TEST(members_surviving_live_support_precedes_discarded)
{
	ClusterMxHistoryGraph *g;
	member_chain(3);
	input.nmembers = 1;
	sources[0].effect = sources[1].effect = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
	g = member_begin(3, true);
	UT_ASSERT(member_assemble(g));
	UT_ASSERT(cluster_undo_record_uba_equal(member_keys[0].locator.uba, sources[2].address));
	UT_ASSERT_EQ(member_proofs[0].effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	graph_clean(g);
}

UT_TEST(members_all_discarded_keeps_newest_top_state)
{
	ClusterMxHistoryGraph *g;
	member_chain(2);
	input.nmembers = 1;
	sources[0].effect = sources[1].effect = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
	g = member_begin(2, true);
	UT_ASSERT(member_assemble(g));
	UT_ASSERT(cluster_undo_record_uba_equal(member_keys[0].locator.uba, sources[0].address));
	UT_ASSERT_EQ(member_proofs[0].base.state, CCM_COMMITTED);
	UT_ASSERT_EQ(member_proofs[0].effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&member_proofs[0].base), 0);
	graph_clean(g);
}

UT_TEST(members_global_order_is_not_page_order)
{
	ClusterMxHistoryGraph *g;
	member_chain(2);
	input.nmembers = 1;
	g = member_begin(2, false);
	UT_ASSERT(!member_assemble(g));
	member_unknown(1);
	graph_clean(g);
}

UT_TEST(members_siblings_of_same_page_prior_are_incomparable)
{
	ClusterMxHistoryGraph *g;
	member_chain(3);
	input.nmembers = 1;
	sources[2].child = 105;
	sources[0].prior = source_head(&sources[2]);
	sources[1].prior = source_head(&sources[2]);
	g = member_begin(3, false);
	UT_ASSERT(!member_assemble(g));
	member_unknown(1);
	graph_clean(g);
}

UT_TEST(members_raw_epoch_ambiguity_rejects_whole_set)
{
	ClusterMxHistoryGraph *g;
	member_chain(3);
	sources[0].child = 104;
	sources[2].child_epoch = 3;
	g = member_begin(3, true);
	UT_ASSERT(!member_assemble(g));
	member_unknown(2);
	graph_clean(g);
}

UT_TEST(members_control_and_other_row_cannot_supply_member)
{
	ClusterMxHistoryGraph *g;
	member_chain(3);
	input.nmembers = 1;
	sources[0].offset = 3;
	sources[1].kind = CMXH_ROLLBACK_CONTROL;
	g = member_begin(3, false);
	UT_ASSERT(member_assemble(g));
	UT_ASSERT(cluster_undo_record_uba_equal(member_keys[0].locator.uba, sources[2].address));
	graph_clean(g);
	member_chain(1);
	input.nmembers = 1;
	sources[0].offset = 3;
	g = member_begin(1, false);
	UT_ASSERT(!member_assemble(g));
	member_unknown(1);
	graph_clean(g);
}

UT_TEST(members_v1_is_exact_top_only)
{
	ClusterMxHistoryGraph *g;
	ClusterCurrentMemberOperationProof extension = { 0 };
	ClusterSavepointOperationKey zero = { 0 };
	member_chain(2);
	input.members[0].xid = 100;
	input.members[1].xid = 103;
	sources[0].kind = CMXH_V1_TOP;
	g = member_begin(2, true);
	UT_ASSERT(member_assemble(g));
	UT_ASSERT_EQ(member_proofs[0].base.member_xid, 100);
	UT_ASSERT_EQ(member_proofs[0].base.state, CCM_COMMITTED);
	extension.base = member_proofs[0].base;
	extension.format_version = 2;
	UT_ASSERT_EQ(memcmp(&extension, &member_proofs[0], sizeof(extension)), 0);
	UT_ASSERT_EQ(memcmp(&zero, &member_keys[0], sizeof(zero)), 0);
	UT_ASSERT_EQ(member_proofs[1].flags, CLUSTER_CURRENT_OPERATION_PROOF_EXACT);
	input.members[0].xid = 104;
	member_descriptor();
	UT_ASSERT(!member_assemble(g));
	member_unknown(2);
	graph_clean(g);
}

UT_TEST(members_peer_echo_and_exact_key_rechecked)
{
	ClusterMxHistoryGraph *g;
	ClusterMxHistoryNodeView v;
	ClusterSavepointOperationKey key;
	member_chain(1);
	input.nmembers = 1;
	g = member_begin(1, true);
	UT_ASSERT(member_assemble(g));
	key = member_keys[0];
	UT_ASSERT(cluster_mx_history_graph_view(g, 0, &v));
	for (unsigned fault = 0; fault < 18; ++fault) {
		ClusterMxHistoryCapture c = input;
		ClusterSavepointOperationKey k = key;
		ClusterCurrentMemberOperationProof p;
		uint16 ordinal = 0;
		switch (fault) {
		case 0:
			c.collector_id++;
			break;
		case 1:
			c.capture_generation++;
			break;
		case 2:
			c.backend_incarnation++;
			break;
		case 3:
			c.snapshot_identity++;
			break;
		case 4:
			c.read_scn++;
			break;
		case 5:
			c.retained_floor++;
			break;
		case 6:
			c.read_epoch++;
			break;
		case 7:
			c.admission.record_generation++;
			break;
		case 8:
			c.descriptor_hash++;
			break;
		case 9:
			c.members[0].member_status = MultiXactStatusForKeyShare;
			break;
		case 10:
			ItemPointerSetOffsetNumber(&c.tid, 3);
			break;
		case 11:
			c.roots[0].write_scn++;
			break;
		case 12:
			k.locator.tt_wrap++;
			break;
		case 13:
			k.locator.itl_slot_index++;
			break;
		case 14:
			k.operation_fxid = FullTransactionIdFromEpochAndXid(3, 103);
			break;
		case 15:
			k.top_fxid = FullTransactionIdFromEpochAndXid(3, 100);
			break;
		case 16:
			k.target.offnum++;
			break;
		case 17:
			ordinal = 1;
			break;
		}
		memset(&p, 0xa5, sizeof(p));
		UT_ASSERT(
			!cluster_multixact_current_operation_proof_from_history_peer(&c, &v, ordinal, &k, &p));
		UT_ASSERT_EQ(p.base.state, CCM_UNKNOWN);
		UT_ASSERT_EQ(p.flags, 0);
	}
	graph_clean(g);
}

UT_TEST(members_active_and_discarded_never_become_self_or_aborted)
{
	for (unsigned discarded = 0; discarded < 2; ++discarded) {
		ClusterMxHistoryGraph *g;
		member_chain(1);
		input.nmembers = 1;
		sources[0].outcome = CLUSTER_TX_IN_PROGRESS;
		sources[0].effect
			= discarded ? CLUSTER_SAVEPOINT_EFFECT_DISCARDED : CLUSTER_SAVEPOINT_EFFECT_LIVE;
		sources[0].grant = discarded ? 0 : 47;
		g = member_begin(1, true);
		UT_ASSERT(member_assemble(g));
		UT_ASSERT_EQ(member_proofs[0].base.state, CCM_ACTIVE);
		UT_ASSERT_EQ(member_proofs[0].base.key.binding_version, 1);
		UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&member_proofs[0].base),
					 discarded ? 0 : 47);
		graph_clean(g);
	}
}

UT_TEST(members_aborted_preserves_independent_disposition)
{
	ClusterMxHistoryGraph *g;
	member_chain(1);
	input.nmembers = 1;
	sources[0].outcome = CLUSTER_TX_ABORTED;
	g = member_begin(1, true);
	UT_ASSERT(member_assemble(g));
	UT_ASSERT_EQ(member_proofs[0].base.state, CCM_ABORTED);
	UT_ASSERT_EQ(member_proofs[0].effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	graph_clean(g);
}

UT_TEST(members_allocation_failure_clears_all_outputs)
{
	ClusterMxHistoryGraph *g;
	member_chain(2);
	sources[0].child = 104;
	g = member_begin(2, true);
	graph_fail_allocation = graph_allocations + 1;
	UT_ASSERT(!member_assemble(g));
	member_unknown(2);
	graph_clean(g);
}

UT_TEST(members_full_descriptor_one_graph)
{
	ClusterMxHistoryGraph *g;
	member_chain(256);
	input.nmembers = 256;
	for (uint16 i = 0; i < 256; ++i) {
		sources[i].child = 1000 + i;
		input.members[255 - i]
			= (ClusterCurrentMxMemberDesc){ 1000 + i, MultiXactStatusForShare, { 0 } };
	}
	g = member_begin(256, true);
	UT_ASSERT(member_assemble(g));
	UT_ASSERT_EQ(graph_fetches, 256);
	for (uint16 i = 0; i < 256; ++i) {
		UT_ASSERT_EQ(member_proofs[i].base.member_xid, input.members[i].xid);
		UT_ASSERT_EQ(member_proofs[i].base.member_ordinal, i);
	}
	graph_clean(g);
}

UT_TEST(members_incomplete_graph_and_short_capacity)
{
	ClusterMxHistoryGraph *g;
	ClusterCurrentMemberOperationProof canary;
	member_chain(2);
	sources[0].child = 104;
	member_descriptor();
	g = graph_begin();
	UT_ASSERT(!member_assemble(g));
	member_unknown(2);
	graph_clean(g);
	member_chain(2);
	sources[0].child = 104;
	g = member_begin(2, true);
	canary = member_proofs[1];
	UT_ASSERT(!cluster_mx_history_assemble_members(&input, g, member_keys, member_proofs, 1));
	member_unknown(1);
	UT_ASSERT_EQ(memcmp(&canary, &member_proofs[1], sizeof(canary)), 0);
	graph_clean(g);
}

UT_TEST(members_update_old_and_new_target_delete_and_lock)
{
	const uint8 types[]
		= { UNDO_RECORD_UPDATE, UNDO_RECORD_UPDATE, UNDO_RECORD_DELETE, UNDO_RECORD_ITL };
	for (unsigned i = 0; i < lengthof(types); ++i) {
		ClusterMxHistoryGraph *g;
		member_chain(1);
		input.nmembers = 1;
		sources[0].record_type = types[i];
		if (i < 2) {
			sources[0].offset = 1;
			if (i == 0)
				ItemPointerSetOffsetNumber(&input.tid, 1);
		}
		/* LOCK_ONLY uses the actual retained lock ITL, not a synthetic
		 * ACTIVE root. Seed through a different tuple's GLOBAL predecessor. */
		if (types[i] == UNDO_RECORD_ITL) {
			GraphSource lock = sources[0];
			free(sources);
			sources = calloc(2, sizeof(*sources));
			source_count = 2;
			sources[0] = source_at(9);
			sources[0].offset = 3;
			sources[1] = lock;
			g = member_begin(2, false);
		} else
			g = member_begin(1, false);
		UT_ASSERT(member_assemble(g));
		UT_ASSERT_EQ(member_keys[0].target.offnum, ItemPointerGetOffsetNumber(&input.tid));
		UT_ASSERT_EQ(member_keys[0].locator.itl_kind,
					 types[i] == UNDO_RECORD_ITL ? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_ACTIVE);
		graph_clean(g);
	}
}

UT_TEST(members_peer_type_identity_and_live_grant_closed)
{
	ClusterMxHistoryGraph *g;
	ClusterMxHistoryNodeView v;
	member_chain(1);
	input.nmembers = 1;
	g = member_begin(1, false);
	UT_ASSERT(member_assemble(g));
	UT_ASSERT(cluster_mx_history_graph_view(g, 0, &v));
	for (unsigned i = 0; i < 13; ++i) {
		ClusterMxHistoryPeer peer = *v.peer;
		ClusterMxHistoryNodeView changed = v;
		ClusterCurrentMemberOperationProof out;
		switch (i) {
		case 0:
			peer.canonical_generation++;
			break;
		case 1:
			peer.canonical.wrap++;
			break;
		case 2:
			peer.top_fxid = FullTransactionIdFromEpochAndXid(3, 100);
			break;
		case 3:
			peer.operation_fxid = FullTransactionIdFromEpochAndXid(3, 103);
			break;
		case 4:
			peer.operation_sequence++;
			break;
		case 5:
			peer.record = uba_encode(257, 10, 3, 0);
			break;
		case 6:
			peer.previous = uba_encode(257, 10, 3, 0);
			break;
		case 7:
			peer.effect = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
			break;
		case 8:
			peer.outcome = CLUSTER_TX_UNKNOWN;
			break;
		case 9:
			peer.outcome = CLUSTER_TX_PREPARED;
			peer.commit_scn = 0;
			break;
		case 10:
			peer.proof_kind = CLUSTER_TX_PROOF_NONE;
			break;
		case 11:
			peer.formation_epoch++;
			break;
		case 12:
			peer.outcome = CLUSTER_TX_IN_PROGRESS;
			peer.commit_scn = 0; /* Missing real LIVE grant. */
			break;
		}
		changed.peer = &peer;
		UT_ASSERT(!cluster_multixact_current_operation_proof_from_history_peer(
			&input, &changed, 0, &member_keys[0], &out));
		UT_ASSERT_EQ(out.base.state, CCM_UNKNOWN);
		UT_ASSERT_EQ(out.flags, 0);
	}
	graph_clean(g);
}

int
main(void)
{
	UT_PLAN(17);
	UT_RUN(members_original_order_and_disposition);
	UT_RUN(members_newest_uses_page_prior_not_lsn);
	UT_RUN(members_surviving_live_support_precedes_discarded);
	UT_RUN(members_all_discarded_keeps_newest_top_state);
	UT_RUN(members_global_order_is_not_page_order);
	UT_RUN(members_siblings_of_same_page_prior_are_incomparable);
	UT_RUN(members_raw_epoch_ambiguity_rejects_whole_set);
	UT_RUN(members_control_and_other_row_cannot_supply_member);
	UT_RUN(members_v1_is_exact_top_only);
	UT_RUN(members_peer_echo_and_exact_key_rechecked);
	UT_RUN(members_active_and_discarded_never_become_self_or_aborted);
	UT_RUN(members_aborted_preserves_independent_disposition);
	UT_RUN(members_allocation_failure_clears_all_outputs);
	UT_RUN(members_full_descriptor_one_graph);
	UT_RUN(members_incomplete_graph_and_short_capacity);
	UT_RUN(members_update_old_and_new_target_delete_and_lock);
	UT_RUN(members_peer_type_identity_and_live_grant_closed);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
