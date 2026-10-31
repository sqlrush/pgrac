/*-------------------------------------------------------------------------
 * test_cluster_tt_exact_batch.c
 *    Actual bounded header provider and single-entry API with a counted
 *    external CR boundary.  The exact slot predicate is the real policy
 *    object.  No transport, C1b finalization or row provenance is simulated
 *    as a positive production certificate.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_segment.h"
#include "storage/bufpage.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

int cluster_node_id = 0;
bool cluster_crossnode_runtime_visibility = true;

static PGAlignedBlock fetched_page;
static ClusterLiveAuthority fetched_authority;
static ClusterVisTtExactRequest requests[CLUSTER_VIS_TT_EXACT_BATCH_MAX];
static bool fetch_ok;
static int fetch_calls;
static int fetched_origin;
static UBA fetched_uba;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* Required by other policy.o functions, not used to fabricate TT facts. */
int
scn_time_cmp(SCN a, SCN b)
{
	return (a > b) - (a < b);
}

bool
cluster_undo_block_fetch_for_visibility(int origin_node, UBA uba, char *out_page,
										ClusterLiveAuthority *auth_out)
{
	fetch_calls++;
	fetched_origin = origin_node;
	fetched_uba = uba;
	/* A failed fetch may have touched outputs; no partial fact may escape. */
	memcpy(out_page, fetched_page.data, BLCKSZ);
	*auth_out = fetched_authority;
	return fetch_ok;
}

#include "test_cluster_tt_exact_batch.inc"

static UndoSegmentHeaderData *
header(void)
{
	return (UndoSegmentHeaderData *)fetched_page.data;
}

static void
reset_fixture(void)
{
	UndoSegmentHeaderData *hdr = header();

	memset(&fetched_page, 0, sizeof(fetched_page));
	memset(requests, 0, sizeof(requests));
	hdr->pd_flags = PD_UNDO_SEG_HEADER;
	hdr->pd_lower = SizeOfPageHeaderData;
	hdr->pd_upper = BLCKSZ;
	hdr->pd_special = BLCKSZ;
	PageSetPageSizeAndVersion((Page)hdr, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	hdr->segment_id = 7;
	hdr->segment_size_bytes = UNDO_SEGMENT_SIZE_BYTES;
	hdr->segment_state = SEGMENT_ACTIVE;
	hdr->owner_instance = 2;
	hdr->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	hdr->wrap_count = 41;
	for (uint32 i = 0; i < CLUSTER_VIS_TT_EXACT_BATCH_MAX; i++) {
		requests[i].slot_offset = i;
		requests[i].xid = 1000 + i;
		requests[i].slot_wrap = 12 + i;
		hdr->tt_slots[i].xid = requests[i].xid;
		hdr->tt_slots[i].wrap = requests[i].slot_wrap;
		hdr->tt_slots[i].status = TT_SLOT_COMMITTED;
		hdr->tt_slots[i].commit_scn = 777 + i;
	}
	fetched_authority.origin_epoch = (UINT64CONST(1) << 40) + 7;
	fetched_authority.live_hwm_lsn = (UINT64CONST(1) << 45) + 5000;
	fetched_authority.tt_generation = (UINT64CONST(1) << 42) + 9;
	fetched_authority.authority_scn = (UINT64CONST(1) << 48) + 900;
	fetch_ok = true;
	fetch_calls = 0;
	fetched_origin = -1;
	memset(&fetched_uba, 0, sizeof(fetched_uba));
	cluster_node_id = 0;
	cluster_crossnode_runtime_visibility = true;
}

static void
assert_empty(const ClusterVisTtExactBatch *out)
{
	ClusterVisTtExactBatch empty;

	memset(&empty, 0, sizeof(empty));
	UT_ASSERT_EQ(memcmp(out, &empty, sizeof(empty)), 0);
}

static void
assert_authority(ClusterLiveAuthority actual)
{
	UT_ASSERT_EQ(actual.origin_epoch, fetched_authority.origin_epoch);
	UT_ASSERT_EQ(actual.live_hwm_lsn, fetched_authority.live_hwm_lsn);
	UT_ASSERT_EQ(actual.tt_generation, fetched_authority.tt_generation);
	UT_ASSERT_EQ(actual.authority_scn, fetched_authority.authority_scn);
}

static void
assert_refused(int origin, uint32 segment, uint32 generation, const ClusterVisTtExactRequest *items,
			   uint32 count, int expected_fetches)
{
	ClusterVisTtExactBatch out;

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_runtime_visibility_remote_exact_tt_batch(origin, segment, generation, items,
																count, &out));
	assert_empty(&out);
	UT_ASSERT_EQ(fetch_calls, expected_fetches);
}

UT_TEST(test_eight_mixed_facts_one_header)
{
	ClusterVisTtExactBatch out;

	reset_fixture();
	UT_ASSERT_EQ(CLUSTER_VIS_TT_EXACT_BATCH_MAX, 8);
	for (int i = 1; i < 8; i += 2) {
		header()->tt_slots[i].status = TT_SLOT_ABORTED;
		header()->tt_slots[i].commit_scn = InvalidScn;
	}
	UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_batch(1, 7, 41, requests, 8, &out));
	UT_ASSERT_EQ(out.count, 8);
	UT_ASSERT_EQ(fetch_calls, 1);
	UT_ASSERT_EQ(fetched_origin, 1);
	/* The only block requested is this segment's header, not a slot/data page. */
	UT_ASSERT_EQ(fetched_uba.raw[0], 7);
	UT_ASSERT_EQ(fetched_uba.raw[1], 0);
	for (int i = 0; i < 8; i++) {
		UT_ASSERT_EQ(out.proofs[i],
					 i % 2 ? CLUSTER_VIS_TT_PROOF_ABORTED : CLUSTER_VIS_TT_PROOF_COMMITTED);
		UT_ASSERT_EQ(out.commit_scns[i], i % 2 ? InvalidScn : (SCN)(777 + i));
	}
	assert_authority(out.authority);
}

UT_TEST(test_request_order_is_preserved)
{
	ClusterVisTtExactBatch out;
	ClusterVisTtExactRequest saved;

	reset_fixture();
	saved = requests[0];
	requests[0] = requests[7];
	requests[7] = saved;
	UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_batch(1, 7, 41, requests, 8, &out));
	UT_ASSERT_EQ(out.commit_scns[0], 784);
	UT_ASSERT_EQ(out.commit_scns[7], 777);
	UT_ASSERT_EQ(fetch_calls, 1);
}

UT_TEST(test_same_xid_distinct_carriers)
{
	ClusterVisTtExactBatch out;

	reset_fixture();
	requests[1].xid = requests[0].xid;
	header()->tt_slots[1].xid = requests[1].xid;
	header()->tt_slots[1].status = TT_SLOT_ABORTED;
	header()->tt_slots[1].commit_scn = InvalidScn;
	UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_batch(1, 7, 41, requests, 2, &out));
	UT_ASSERT_EQ(out.count, 2);
	UT_ASSERT_EQ(out.proofs[0], CLUSTER_VIS_TT_PROOF_COMMITTED);
	UT_ASSERT_EQ(out.commit_scns[0], 777);
	UT_ASSERT_EQ(out.proofs[1], CLUSTER_VIS_TT_PROOF_ABORTED);
	UT_ASSERT_EQ(out.commit_scns[1], InvalidScn);
	UT_ASSERT_EQ(fetch_calls, 1);
}

UT_TEST(test_repeated_exact_request)
{
	ClusterVisTtExactBatch out;

	reset_fixture();
	requests[1] = requests[0];
	UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_batch(1, 7, 41, requests, 2, &out));
	UT_ASSERT_EQ(out.count, 2);
	UT_ASSERT_EQ(out.proofs[0], CLUSTER_VIS_TT_PROOF_COMMITTED);
	UT_ASSERT_EQ(out.proofs[1], CLUSTER_VIS_TT_PROOF_COMMITTED);
	UT_ASSERT_EQ(out.commit_scns[0], 777);
	UT_ASSERT_EQ(out.commit_scns[1], 777);
	UT_ASSERT_EQ(fetch_calls, 1);
}

UT_TEST(test_last_identity_mismatch_clears_batch)
{
	for (int variant = 0; variant < 2; variant++) {
		reset_fixture();
		if (variant == 0)
			header()->tt_slots[7].xid++;
		else
			header()->tt_slots[7].wrap++;
		assert_refused(1, 7, 41, requests, 8, 1);
	}
}

UT_TEST(test_generation_drift_clears_batch)
{
	reset_fixture();
	header()->wrap_count++;
	assert_refused(1, 7, 41, requests, 8, 1);
}

UT_TEST(test_header_identity_and_layout_refuse)
{
	for (int variant = 0; variant < 8; variant++) {
		reset_fixture();
		switch (variant) {
		case 0:
			header()->owner_instance = 3;
			break;
		case 1:
			header()->segment_id = 8;
			break;
		case 2:
			header()->segment_size_bytes = 0;
			break;
		case 3:
			header()->tt_slots_count--;
			break;
		case 4:
			header()->pd_flags = 0;
			break;
		case 5:
			header()->pd_pagesize_version = 0;
			break;
		case 6:
			header()->segment_state = UINT8_MAX;
			break;
		case 7:
			header()->tt_slots_count++;
			break;
		}
		assert_refused(1, 7, 41, requests, 2, 1);
	}
}

UT_TEST(test_nonterminal_slot_refuses_batch)
{
	const uint8 states[] = { TT_SLOT_UNUSED, TT_SLOT_ACTIVE, TT_SLOT_RECYCLABLE };

	for (int i = 0; i < lengthof(states); i++) {
		reset_fixture();
		header()->tt_slots[1].status = states[i];
		assert_refused(1, 7, 41, requests, 2, 1);
	}
}

UT_TEST(test_inconsistent_terminal_facts_refuse)
{
	for (int variant = 0; variant < 4; variant++) {
		reset_fixture();
		switch (variant) {
		case 0:
			header()->tt_slots[1].commit_scn = InvalidScn;
			break;
		case 1:
			header()->tt_slots[1].status = TT_SLOT_ABORTED;
			break;
		case 2:
			header()->tt_slots[1].flags = UINT8_C(0x80);
			break;
		case 3:
			header()->tt_slots[1].status = TT_SLOT_INVALID;
			break;
		}
		assert_refused(1, 7, 41, requests, 2, 1);
	}
}

UT_TEST(test_count_and_pointer_reject_before_fetch)
{
	const uint32 counts[] = { 0, 9, UINT32_MAX };

	for (int i = 0; i < lengthof(counts); i++) {
		reset_fixture();
		assert_refused(1, 7, 41, requests, counts[i], 0);
	}
	reset_fixture();
	assert_refused(1, 7, 41, NULL, 1, 0);
}

UT_TEST(test_invalid_last_request_rejects_before_fetch)
{
	for (int variant = 0; variant < 6; variant++) {
		reset_fixture();
		switch (variant) {
		case 0:
			requests[7].slot_offset = TT_SLOTS_PER_SEGMENT;
			break;
		case 1:
			requests[7].slot_offset = UINT16_MAX;
			break;
		case 2:
			requests[7].xid = InvalidTransactionId;
			break;
		case 3:
			requests[7].xid = BootstrapTransactionId;
			break;
		case 4:
			requests[7].xid = FrozenTransactionId;
			break;
		case 5:
			requests[7].slot_wrap = UINT16_MAX;
			break;
		}
		assert_refused(1, 7, 41, requests, 8, 0);
	}
}

UT_TEST(test_common_identity_rejects_before_fetch)
{
	const int origins[] = { -1, CLUSTER_MAX_NODES, 0 };

	for (int i = 0; i < lengthof(origins); i++) {
		reset_fixture();
		assert_refused(origins[i], 7, 41, requests, 1, 0);
	}
	reset_fixture();
	assert_refused(1, 0, 41, requests, 1, 0);
	assert_refused(1, (uint32)UINT16_MAX + 1, 41, requests, 1, 0);
	assert_refused(1, 7, UINT32_MAX, requests, 1, 0);
}

UT_TEST(test_disabled_feature_rejects_before_fetch)
{
	reset_fixture();
	cluster_crossnode_runtime_visibility = false;
	assert_refused(1, 7, 41, requests, 1, 0);
}

UT_TEST(test_fetch_failure_clears_all_output)
{
	reset_fixture();
	fetch_ok = false;
	assert_refused(1, 7, 41, requests, 8, 1);
}

UT_TEST(test_null_output_rejects_before_fetch)
{
	reset_fixture();
	UT_ASSERT(!cluster_runtime_visibility_remote_exact_tt_batch(1, 7, 41, requests, 1, NULL));
	UT_ASSERT_EQ(fetch_calls, 0);
}

UT_TEST(test_single_entry_terminal_compatibility)
{
	for (int aborted = 0; aborted < 2; aborted++) {
		ClusterVisTtProof proof;
		SCN scn;
		ClusterLiveAuthority auth;

		reset_fixture();
		if (aborted) {
			header()->tt_slots[0].status = TT_SLOT_ABORTED;
			header()->tt_slots[0].commit_scn = InvalidScn;
		}
		UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_proof(1, 7, 41, 0, 1000, 12, &proof,
																   &scn, &auth));
		UT_ASSERT_EQ(proof,
					 aborted ? CLUSTER_VIS_TT_PROOF_ABORTED : CLUSTER_VIS_TT_PROOF_COMMITTED);
		UT_ASSERT_EQ(scn, aborted ? InvalidScn : (SCN)777);
		assert_authority(auth);
		UT_ASSERT_EQ(fetch_calls, 1);
	}
}

UT_TEST(test_single_entry_refusal_clears_outputs)
{
	ClusterVisTtProof proof = CLUSTER_VIS_TT_PROOF_COMMITTED;
	SCN scn = 999;
	ClusterLiveAuthority auth;
	ClusterLiveAuthority empty;

	reset_fixture();
	memset(&auth, 0xa5, sizeof(auth));
	memset(&empty, 0, sizeof(empty));
	UT_ASSERT(!cluster_runtime_visibility_remote_exact_tt_proof(1, 7, 41, 0, 1000, 13, &proof, &scn,
																&auth));
	UT_ASSERT_EQ(proof, CLUSTER_VIS_TT_PROOF_NONE);
	UT_ASSERT_EQ(scn, InvalidScn);
	UT_ASSERT_EQ(memcmp(&auth, &empty, sizeof(auth)), 0);
	UT_ASSERT_EQ(fetch_calls, 1);
}

UT_TEST(test_single_entry_requires_all_outputs)
{
	for (int missing = 0; missing < 3; missing++) {
		ClusterVisTtProof proof = CLUSTER_VIS_TT_PROOF_COMMITTED;
		SCN scn = 999;
		ClusterLiveAuthority auth;
		ClusterLiveAuthority empty;

		reset_fixture();
		memset(&auth, 0xa5, sizeof(auth));
		memset(&empty, 0, sizeof(empty));
		UT_ASSERT(!cluster_runtime_visibility_remote_exact_tt_proof(
			1, 7, 41, 0, 1000, 12, missing == 0 ? NULL : &proof, missing == 1 ? NULL : &scn,
			missing == 2 ? NULL : &auth));
		if (missing != 0)
			UT_ASSERT_EQ(proof, CLUSTER_VIS_TT_PROOF_NONE);
		if (missing != 1)
			UT_ASSERT_EQ(scn, InvalidScn);
		if (missing != 2)
			UT_ASSERT_EQ(memcmp(&auth, &empty, sizeof(auth)), 0);
		UT_ASSERT_EQ(fetch_calls, 0);
	}
}

UT_TEST(test_valid_domain_boundaries)
{
	ClusterVisTtExactBatch out;

	reset_fixture();
	requests[0].slot_offset = TT_SLOTS_PER_SEGMENT - 1;
	requests[0].xid = UINT32_MAX;
	requests[0].slot_wrap = TT_WRAP_MAX;
	header()->segment_id = UINT16_MAX;
	header()->owner_instance = CLUSTER_MAX_NODES;
	header()->wrap_count = UINT32_MAX - 1;
	header()->tt_slots[TT_SLOTS_PER_SEGMENT - 1].xid = UINT32_MAX;
	header()->tt_slots[TT_SLOTS_PER_SEGMENT - 1].wrap = TT_WRAP_MAX;
	header()->tt_slots[TT_SLOTS_PER_SEGMENT - 1].status = TT_SLOT_ABORTED;
	UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_batch(CLUSTER_MAX_NODES - 1, UINT16_MAX,
															   UINT32_MAX - 1, requests, 1, &out));
	UT_ASSERT_EQ(out.count, 1);
	UT_ASSERT_EQ(out.proofs[0], CLUSTER_VIS_TT_PROOF_ABORTED);
	UT_ASSERT_EQ(out.commit_scns[0], InvalidScn);
	UT_ASSERT_EQ(fetch_calls, 1);
	UT_ASSERT_EQ(fetched_origin, 127);
	UT_ASSERT_EQ(fetched_uba.raw[0], 65535);
	UT_ASSERT_EQ(fetched_uba.raw[1], 0);
}

UT_TEST(test_shorter_batch_clears_unused_tail)
{
	ClusterVisTtExactBatch out;

	reset_fixture();
	UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_batch(1, 7, 41, requests, 8, &out));
	UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_batch(1, 7, 41, requests, 1, &out));
	UT_ASSERT_EQ(out.count, 1);
	UT_ASSERT_EQ(out.proofs[0], CLUSTER_VIS_TT_PROOF_COMMITTED);
	for (int i = 1; i < 8; i++) {
		UT_ASSERT_EQ(out.proofs[i], CLUSTER_VIS_TT_PROOF_NONE);
		UT_ASSERT_EQ(out.commit_scns[i], InvalidScn);
	}
	UT_ASSERT_EQ(fetch_calls, 2);
}

static UndoSegmentHeaderDataV2 *
reset_v2_fixture(void)
{
	UndoSegmentHeaderDataV2 *hdr = (UndoSegmentHeaderDataV2 *)fetched_page.data;
	const uint32 epochs[] = { 0, 1, UINT32_MAX, 7 };

	reset_fixture();
	memset(fetched_page.data + 112, 0, BLCKSZ - 112);
	hdr->tt_layout_version = CLUSTER_UNDO_TT_LAYOUT_V2;
	hdr->tt_slot_bytes = sizeof(TTSlotV2);
	hdr->wrap_count = 41;
	for (uint32 i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		TTSlotV2 *slot = &hdr->tt_slots[i];

		slot->xid = 1000 + i;
		slot->wrap = 12 + i;
		slot->status = i % 2 ? TT_SLOT_ABORTED : TT_SLOT_COMMITTED;
		slot->commit_scn = i % 2 ? InvalidScn : (SCN)(777 + i);
		slot->original_full_xid = ((uint64)epochs[i % lengthof(epochs)] << 32) | slot->xid;
	}
	/* The V1 generation offset is in the V2 bitmap, never its generation. */
	header()->wrap_count = 99;
	return hdr;
}

UT_TEST(test_v2_all_slots_publish_exact_persisted_identity)
{
	UndoSegmentHeaderDataV2 *hdr = reset_v2_fixture();
	ClusterVisTtExactBatch out;

	for (uint32 first = 0; first < TT_SLOTS_PER_SEGMENT; first += 8) {
		for (uint32 i = 0; i < 8; i++) {
			const TTSlotV2 *slot = &hdr->tt_slots[first + i];

			requests[i].slot_offset = first + i;
			requests[i].xid = slot->xid;
			requests[i].slot_wrap = slot->wrap;
		}
		UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_batch(1, 7, 41, requests, 8, &out));
		UT_ASSERT_EQ(out.count, 8);
		for (uint32 i = 0; i < 8; i++) {
			const TTSlotV2 *slot = &hdr->tt_slots[first + i];

			UT_ASSERT_EQ(out.proofs[i], i % 2 ? CLUSTER_VIS_TT_PROOF_ABORTED
										   : CLUSTER_VIS_TT_PROOF_COMMITTED);
			UT_ASSERT_EQ(out.commit_scns[i], slot->commit_scn);
			UT_ASSERT_EQ(U64FromFullTransactionId(out.original_full_xids[i]),
						 slot->original_full_xid);
		}
		assert_authority(out.authority);
		UT_ASSERT_EQ(fetch_calls, first / 8 + 1);
	}
}

UT_TEST(test_v2_rejects_bad_framing_before_slot_use)
{
	for (int variant = 0; variant < 11; variant++) {
		UndoSegmentHeaderDataV2 *hdr = reset_v2_fixture();

		switch (variant) {
		case 0:
			hdr->tt_layout_version = 3;
			break;
		case 1:
			hdr->tt_layout_version = 0;
			break;
		case 2:
			hdr->tt_slot_bytes = sizeof(TTSlot);
			break;
		case 3:
			hdr->tt_slot_bytes = 0;
			break;
		case 4:
			hdr->tt_layout_reserved = 1;
			break;
		case 5:
			hdr->tt_next_free_slot = TT_SLOTS_PER_SEGMENT + 1;
			break;
		case 6:
			hdr->tt_active_count = TT_SLOTS_PER_SEGMENT + 1;
			break;
		case 7:
			hdr->tt_slots_count--;
			break;
		case 8:
			hdr->owner_instance++;
			break;
		case 9:
			hdr->pd_pagesize_version = 0;
			break;
		case 10:
			hdr->segment_state = UINT8_MAX;
			break;
		}
		assert_refused(1, 7, 41, requests, 8, 1);
	}
}

UT_TEST(test_v2_bad_last_identity_clears_full_batch)
{
	for (int variant = 0; variant < 8; variant++) {
		UndoSegmentHeaderDataV2 *hdr = reset_v2_fixture();
		TTSlotV2 *slot = &hdr->tt_slots[7];

		switch (variant) {
		case 0:
			slot->original_full_xid = 0;
			break;
		case 1:
			slot->original_full_xid = UINT64_MAX;
			break;
		case 2:
			slot->original_full_xid++;
			break;
		case 3:
			slot->original_full_xid = UINT64CONST(0x100000002);
			break;
		case 4:
			slot->wrap++;
			break;
		case 5:
			slot->status = TT_SLOT_INVALID;
			break;
		case 6:
			slot->flags = UINT8_C(0x80);
			break;
		case 7:
			slot->commit_scn = UINT64_MAX;
			break;
		}
		assert_refused(1, 7, 41, requests, 8, 1);
	}
}

UT_TEST(test_v2_raw_aliases_keep_distinct_full_identity)
{
	UndoSegmentHeaderDataV2 *hdr = reset_v2_fixture();
	ClusterVisTtExactBatch out;

	hdr->tt_slots[1].xid = requests[0].xid;
	requests[1].xid = requests[0].xid;
	hdr->tt_slots[1].original_full_xid = (UINT64CONST(9) << 32) | requests[0].xid;
	UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_batch(1, 7, 41, requests, 2, &out));
	UT_ASSERT_EQ(U64FromFullTransactionId(out.original_full_xids[0]), requests[0].xid);
	UT_ASSERT_EQ(U64FromFullTransactionId(out.original_full_xids[1]),
				 hdr->tt_slots[1].original_full_xid);
	UT_ASSERT(!FullTransactionIdEquals(out.original_full_xids[0], out.original_full_xids[1]));
	UT_ASSERT_EQ(fetch_calls, 1);
	/* Refuse newer segment generations, even with the same raw/full bytes. */
	hdr->wrap_count++;
	assert_refused(1, 7, 41, requests, 2, 2);
}

UT_TEST(test_v2_single_entry_uses_same_exact_reader)
{
	UndoSegmentHeaderDataV2 *hdr = reset_v2_fixture();
	ClusterVisTtProof proof;
	SCN scn;
	ClusterLiveAuthority auth;

	UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_proof(1, 7, 41, 46, 1046, 58,
															  &proof, &scn, &auth));
	UT_ASSERT_EQ(proof, CLUSTER_VIS_TT_PROOF_COMMITTED);
	UT_ASSERT_EQ(scn, 823);
	assert_authority(auth);
	hdr->tt_slots[46].original_full_xid++;
	UT_ASSERT(!cluster_runtime_visibility_remote_exact_tt_proof(1, 7, 41, 46, 1046, 58,
															   &proof, &scn, &auth));
	UT_ASSERT_EQ(proof, CLUSTER_VIS_TT_PROOF_NONE);
	UT_ASSERT_EQ(scn, InvalidScn);
	UT_ASSERT_EQ(auth.origin_epoch, 0);
	UT_ASSERT_EQ(fetch_calls, 2);
}

UT_TEST(test_legacy_and_short_batch_never_invent_full_identity)
{
	ClusterVisTtExactBatch out;

	reset_v2_fixture();
	UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_batch(1, 7, 41, requests, 8, &out));
	UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_batch(1, 7, 41, requests, 1, &out));
	for (uint32 i = 1; i < 8; i++)
		UT_ASSERT_EQ(U64FromFullTransactionId(out.original_full_xids[i]), 0);
	reset_fixture();
	UT_ASSERT(cluster_runtime_visibility_remote_exact_tt_batch(1, 7, 41, requests, 8, &out));
	for (uint32 i = 0; i < 8; i++)
		UT_ASSERT_EQ(U64FromFullTransactionId(out.original_full_xids[i]), 0);
}

int
main(void)
{
	UT_PLAN(26);
	UT_RUN(test_eight_mixed_facts_one_header);
	UT_RUN(test_request_order_is_preserved);
	UT_RUN(test_same_xid_distinct_carriers);
	UT_RUN(test_repeated_exact_request);
	UT_RUN(test_last_identity_mismatch_clears_batch);
	UT_RUN(test_generation_drift_clears_batch);
	UT_RUN(test_header_identity_and_layout_refuse);
	UT_RUN(test_nonterminal_slot_refuses_batch);
	UT_RUN(test_inconsistent_terminal_facts_refuse);
	UT_RUN(test_count_and_pointer_reject_before_fetch);
	UT_RUN(test_invalid_last_request_rejects_before_fetch);
	UT_RUN(test_common_identity_rejects_before_fetch);
	UT_RUN(test_disabled_feature_rejects_before_fetch);
	UT_RUN(test_fetch_failure_clears_all_output);
	UT_RUN(test_null_output_rejects_before_fetch);
	UT_RUN(test_single_entry_terminal_compatibility);
	UT_RUN(test_single_entry_refusal_clears_outputs);
	UT_RUN(test_single_entry_requires_all_outputs);
	UT_RUN(test_valid_domain_boundaries);
	UT_RUN(test_shorter_batch_clears_unused_tail);
	UT_RUN(test_v2_all_slots_publish_exact_persisted_identity);
	UT_RUN(test_v2_rejects_bad_framing_before_slot_use);
	UT_RUN(test_v2_bad_last_identity_clears_full_batch);
	UT_RUN(test_v2_raw_aliases_keep_distinct_full_identity);
	UT_RUN(test_v2_single_entry_uses_same_exact_reader);
	UT_RUN(test_legacy_and_short_batch_never_invent_full_identity);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
