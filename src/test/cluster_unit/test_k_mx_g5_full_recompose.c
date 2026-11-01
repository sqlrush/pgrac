/* Full G2 assembly into a pure plan. Literal ACTIVE envelopes prove only
 * structure, never native grant/holder/H qualification or publication. */
int k_full_recompose_fixture_main(void);
#define main k_full_recompose_fixture_main
#include "test_cluster_mx_composition.c"
#undef main

static ClusterMxCompositionReceive *
k_plan_input_status(uint32 total, bool active, bool tail_only, bool updater,
			 bool finish_proofs, ClusterMxCompositionReceiveStats *stats,
			 MultiXactStatus updater_status)
{
	ClusterMxCompositionReceive *op = begin(total, stats);
	ClusterCurrentMxMemberDesc members[256];
	ClusterMxFullMemberProof proofs[7];
	for (uint32 first = 0; first < total;) {
		uint32 n = Min(total - first, 256);
		ClusterMxCompositionReply r = reply(total, first, n, members);
		for (uint32 i = 0; i < n; i++)
			members[i].member_status = updater && first + i + 1 == total
				? updater_status : MultiXactStatusForShare;
		UT_ASSERT_EQ(cluster_mx_composition_accept(&op, MXCOMP_READ_OK, &r),
			r.last ? MXCOMP_COMPLETE : MXCOMP_PARTIAL);
		first += n;
	}
	for (uint32 first = 0; first < total;) {
		uint32 n = Min(total - first, 7);
		ClusterMxProofReply r = {0};
		if (!finish_proofs && first + n == total) break;
		r.source = source; r.source_bytes = sizeof(source); r.total = total;
		r.first = first; r.count = n; r.next = first + n; r.last = r.next == total;
		r.proofs = proofs; r.proof_bytes = (Size)n * sizeof(*proofs);
		r.frame_bytes = sizeof(source) + r.proof_bytes;
		for (uint32 i = 0; i < n; i++) {
			ClusterCurrentMemberProof *q = &proofs[i].proof;
			bool live = active && (!tail_only || first + i + 1 == total);
			memset(&proofs[i], 0, sizeof(proofs[i]));
			proofs[i].member_ordinal = first + i; proofs[i].origin_node_id = 2;
			q->member_xid = 100 + first + i; q->member_ordinal = i;
			q->member_status = updater && first + i + 1 == total
				? updater_status : MultiXactStatusForShare;
			q->state = live ? CCM_ACTIVE : CCM_COMMITTED;
			if (live) {
				q->key.origin_node_id = 2; q->key.undo_segment_id = 1;
				q->key.tt_slot_id = 1; q->key.cluster_epoch = 9;
				q->key.local_xid = q->member_xid;
				ClusterCurrentMemberProofSetCtrcBinding(q, 17, 3);
				ClusterCurrentMemberProofSetCtrcGrant(q, 7);
				/* Structural V2 header only, not an authority fixture. */
				q->origin_fullxid_v2[0] = CLUSTER_CTRC_ORIGIN_FULLXID_V2_VERSION;
				q->origin_fullxid_v2[2] = CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES;
			} else q->commit_scn = 1000 + first + i;
		}
		UT_ASSERT_EQ(cluster_mx_composition_accept_proofs(&op, MXCOMP_READ_OK, &r),
			r.last ? MXCOMP_COMPLETE : MXCOMP_PARTIAL);
		first += n;
	}
	return op;
}

static ClusterMxCompositionReceive *
k_plan_input(uint32 total, bool active, bool tail_only, bool updater,
			 bool finish_proofs, ClusterMxCompositionReceiveStats *stats)
{
	return k_plan_input_status(total, active, tail_only, updater, finish_proofs,
		stats, MultiXactStatusUpdate);
}

static MultiXactMember **
k_output(uint32 cap)
{
	uint32 pages = (cap + 255) / 256;
	MultiXactMember **out = calloc(pages, sizeof(*out));
	if (!out) abort();
	for (uint32 i = 0; i < pages; i++) {
		out[i] = malloc(256 * sizeof(**out)); if (!out[i]) abort();
		memset(out[i], 0xa5, 256 * sizeof(**out));
	}
	return out;
}
static void
k_output_free(MultiXactMember **out, uint32 cap)
{
	for (uint32 i = 0; i < (cap + 255) / 256; i++) free(out[i]);
	free(out);
}
static void
k_zero_output(MultiXactMember **out, uint32 cap)
{
	static const MultiXactMember zero;
	for (uint32 i = 0; i < cap; i++)
		UT_ASSERT_EQ(memcmp(&out[i / 256][i % 256], &zero, sizeof(zero)), 0);
}

UT_TEST(terminal_sets_recompose_through_65537_real_members)
{
	const uint32 sizes[] = {2, 256, 257, 65537};
	for (unsigned s = 0; s < lengthof(sizes); s++) {
		ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = k_plan_input(sizes[s], false, false, false, true, &stats);
		MultiXactMember **out = k_output(3); uint32 n = 99;
		UT_ASSERT_EQ(cluster_mx_composition_recompose(op, 7, MultiXactStatusForShare,
			out, 3, &n), CMX_RECOMPOSE_OK);
		UT_ASSERT_EQ(n, 1); UT_ASSERT_EQ(out[0][0].xid, 7);
		UT_ASSERT_EQ(out[0][0].status, MultiXactStatusForShare);
		UT_ASSERT_EQ(out[0][1].xid, 0); UT_ASSERT_EQ(out[0][2].xid, 0);
		k_output_free(out, 3); cluster_mx_composition_finish(&op); check_discarded(op);
	}
}
UT_TEST(all_live_members_and_new_requester_cross_output_pages)
{
	const uint32 sizes[] = {256, 65537};
	for (unsigned s = 0; s < lengthof(sizes); s++) {
		uint32 cap = sizes[s] + 1, n = 99;
		ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = k_plan_input(sizes[s], true, false, false, true, &stats);
		MultiXactMember **out = k_output(cap);
		UT_ASSERT_EQ(cluster_mx_composition_recompose(op, 7, MultiXactStatusForShare,
			out, cap, &n), CMX_RECOMPOSE_OK);
		UT_ASSERT_EQ(n, cap);
		for (uint32 i = 0; i < sizes[s]; i++) {
			UT_ASSERT_EQ(out[i / 256][i % 256].xid, 100 + i);
			UT_ASSERT_EQ(out[i / 256][i % 256].status, MultiXactStatusForShare);
		}
		UT_ASSERT_EQ(out[(cap - 1) / 256][(cap - 1) % 256].xid, 7);
		UT_ASSERT_EQ(out[(cap - 1) / 256][(cap - 1) % 256].status, MultiXactStatusForShare);
		k_output_free(out, cap); cluster_mx_composition_finish(&op); check_discarded(op);
	}
}
UT_TEST(tail_requester_keeps_position_and_merges_lock_strength)
{
	const uint32 total = 65537; uint32 n = 99;
	ClusterMxCompositionReceiveStats stats = {0};
	ClusterMxCompositionReceive *op = k_plan_input(total, true, false, false, true, &stats);
	MultiXactMember **out = k_output(total);
	UT_ASSERT_EQ(cluster_mx_composition_recompose(op, 100 + total - 1, MultiXactStatusForUpdate,
		out, total, &n), CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(n, total);
	for (uint32 i = 0; i < total; i++) {
		UT_ASSERT_EQ(out[i / 256][i % 256].xid, 100 + i);
		UT_ASSERT_EQ(out[i / 256][i % 256].status,
			i + 1 == total ? MultiXactStatusForUpdate : MultiXactStatusForShare);
	}
	k_output_free(out, total); cluster_mx_composition_finish(&op); check_discarded(op);
}
UT_TEST(terminal_prefix_never_hides_live_member_at_65536)
{
	uint32 n = 99; ClusterMxCompositionReceiveStats stats = {0};
	ClusterMxCompositionReceive *op = k_plan_input(65537, true, true, false, true, &stats);
	MultiXactMember **out = k_output(2);
	UT_ASSERT_EQ(cluster_mx_composition_recompose(op, 7, MultiXactStatusForShare,
		out, 2, &n), CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(n, 2); UT_ASSERT_EQ(out[0][0].xid, 65636);
	UT_ASSERT_EQ(out[0][1].xid, 7);
	k_output_free(out, 2); cluster_mx_composition_finish(&op); check_discarded(op);
}
UT_TEST(bad_final_proof_discards_every_output_page)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		uint32 n = 99, total = 0; ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = k_plan_input(257, true, false, false, true, &stats);
		const ClusterMxCompositionProofPage *last = cluster_mx_composition_proofs(op, &total);
		MultiXactMember **out = k_output(258);
		ClusterMxFullMemberProof *q;
		while (last && last->next) last = last->next;
		if (!last) abort();
		/* Consumer fault injection after G2 completion, never a source of proof. */
		q = (ClusterMxFullMemberProof *)&last->proofs[last->count - 1];
		switch (fault) {
		case 0: q->proof.state = CCM_UNKNOWN; break;
		case 1: q->member_ordinal = 0; break;
		case 2: q->proof.member_ordinal = 6; break;
		case 3: q->proof.member_xid++; break;
		case 4: q->reserved16 = 1; break;
		case 5: q->proof.commit_scn = 1; break;
		case 6: ClusterCurrentMemberProofSetCtrcGrant(&q->proof, 0); break;
		}
		UT_ASSERT_EQ(cluster_mx_composition_recompose(op, 7, MultiXactStatusForShare,
			out, 258, &n), CMX_RECOMPOSE_UNKNOWN);
		UT_ASSERT_EQ(n, 0); k_zero_output(out, 258);
		k_output_free(out, 258); cluster_mx_composition_finish(&op); check_discarded(op);
	}
}
UT_TEST(short_output_has_no_partial_plan_or_member_count_gate)
{
	uint32 n = 99; ClusterMxCompositionReceiveStats stats = {0};
	ClusterMxCompositionReceive *op = k_plan_input(256, true, false, false, true, &stats);
	MultiXactMember **out = k_output(256);
	UT_ASSERT_EQ(cluster_mx_composition_recompose(op, 7, MultiXactStatusForShare,
		out, 256, &n), CMX_RECOMPOSE_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(n, 0); k_zero_output(out, 256);
	k_output_free(out, 256);
	out = k_output(257);
	UT_ASSERT_EQ(cluster_mx_composition_recompose(op, 7, MultiXactStatusForShare,
		out, 257, &n), CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(n, 257);
	k_output_free(out, 257); cluster_mx_composition_finish(&op); check_discarded(op);
}
UT_TEST(incomplete_proofs_and_invalid_requester_clear_output)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		uint32 n = 99; ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = k_plan_input(257, false, false, false, fault != 0, &stats);
		MultiXactMember **out = k_output(258);
		UT_ASSERT_EQ(cluster_mx_composition_recompose(op, fault == 1 ? 1 : 7,
			fault == 2 ? MaxMultiXactStatus + 1 : MultiXactStatusForShare,
			out, 258, &n), CMX_RECOMPOSE_UNKNOWN);
		UT_ASSERT_EQ(n, 0); k_zero_output(out, 258);
		k_output_free(out, 258); cluster_mx_composition_finish(&op); check_discarded(op);
	}
}
UT_TEST(updater_rules_are_preserved_across_pages)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		uint32 n = 99; ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = k_plan_input(257, mode != 0, false, true, true, &stats);
		MultiXactMember **out = k_output(258);
		UT_ASSERT_EQ(cluster_mx_composition_recompose(op, mode == 2 ? 356 : 7,
			mode == 1 ? MultiXactStatusUpdate : MultiXactStatusForShare, out, 258, &n),
			mode == 2 ? CMX_RECOMPOSE_OK : CMX_RECOMPOSE_DENIED);
		if (mode == 2) {
			UT_ASSERT_EQ(n, 257); UT_ASSERT_EQ(out[1][0].xid, 356);
			UT_ASSERT_EQ(out[1][0].status, MultiXactStatusUpdate);
		} else { UT_ASSERT_EQ(n, 0); k_zero_output(out, 258); }
		k_output_free(out, 258); cluster_mx_composition_finish(&op); check_discarded(op);
	}
}
#ifndef K_MX_FULL_RECOMPOSE_MAIN
#define K_MX_FULL_RECOMPOSE_MAIN main
#endif
int K_MX_FULL_RECOMPOSE_MAIN(void)
{
	UT_PLAN(8);
	UT_RUN(terminal_sets_recompose_through_65537_real_members);
	UT_RUN(all_live_members_and_new_requester_cross_output_pages);
	UT_RUN(tail_requester_keeps_position_and_merges_lock_strength);
	UT_RUN(terminal_prefix_never_hides_live_member_at_65536);
	UT_RUN(bad_final_proof_discards_every_output_page);
	UT_RUN(short_output_has_no_partial_plan_or_member_count_gate);
	UT_RUN(incomplete_proofs_and_invalid_requester_clear_output);
	UT_RUN(updater_rules_are_preserved_across_pages);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
