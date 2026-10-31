/* Author: SqlRush <sqlrush@gmail.com> */
/* Execute the original legacy rollback entry. Undo reads and chain mutation
 * are explicit boundaries; shared refusal must precede both. */
int durable_fixture_main(int argc, char **argv);
struct ClusterResId;
#define main durable_fixture_main
#include "test_cluster_tt_durable.c"
#undef main

bool cluster_enabled = true;
bool cluster_shared_config;
bool cluster_tt_recovery_resolve_active = true;
static unsigned rollback_reads, rollback_chains;
static bool startup_page_present;
static uint32 startup_segment;
static PGAlignedBlock startup_page;
static SCN startup_observed_scn;
static unsigned startup_observes, startup_highwater_counts;
struct TTRevertExpect;
static int rollback_chain(UBA head, const struct TTRevertExpect *expected);

void
cluster_scn_recovery_replay_observe(SCN scn)
{
	UT_ASSERT_EQ(rollback_reads, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	startup_observed_scn = scn;
	startup_observes++;
}

void
cluster_tt_recovery_count_scn_highwater_recovered(void)
{
	startup_highwater_counts++;
}

static bool
rollback_read(ClusterUndoPathIntent intent, uint32 segment, uint8 owner, BlockNumber block,
			  void *buffer)
{
	rollback_reads++;
	if (startup_page_present && segment == startup_segment) {
		UT_ASSERT_EQ(intent, cluster_undo_intent_for_owner(owner));
		UT_ASSERT_EQ(owner, (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1);
		UT_ASSERT_EQ(block, 0);
		memcpy(buffer, startup_page.data, BLCKSZ);
		return true;
	}
	return false;
}

#define cluster_undo_smgr_read_block rollback_read
#define revert_aborted_undo_chain rollback_chain
#include "test_cluster_tt_rollback_entry.inc"
#undef revert_aborted_undo_chain
#undef cluster_undo_smgr_read_block

static TTRevertExpect startup_chain_expect[TT_SLOTS_PER_SEGMENT];
static UBA startup_chain_head[TT_SLOTS_PER_SEGMENT];

static int
rollback_chain(UBA head, const struct TTRevertExpect *expected)
{
	UT_ASSERT(rollback_chains < TT_SLOTS_PER_SEGMENT);
	startup_chain_expect[rollback_chains] = *expected;
	startup_chain_head[rollback_chains] = head;
	rollback_chains++;
	return 0;
}

UT_TEST(test_shared_legacy_rollback_refuses_before_any_undo_read)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		volatile bool refused = false;
		cluster_shared_config = true;
		cluster_enabled = mode != 1;
		cluster_tt_recovery_resolve_active = mode != 2;
		cluster_node_id = mode == 3 ? -1 : 0;
		rollback_reads = rollback_chains = 0;
		last_ereport_elevel = last_ereport_errcode = 0;
		PG_TRY();
		{
			(void)cluster_tt_recovery_physical_rollback();
		}
		PG_CATCH();
		{
			refused = true;
		}
		PG_END_TRY();
		UT_ASSERT(refused);
		UT_ASSERT_EQ(last_ereport_elevel, ERROR);
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(rollback_reads, 0);
		UT_ASSERT_EQ(rollback_chains, 0);
	}
}

UT_TEST(test_nonshared_legacy_rollback_keeps_original_scan)
{
	cluster_shared_config = false;
	cluster_enabled = cluster_tt_recovery_resolve_active = true;
	cluster_node_id = 0;
	rollback_reads = rollback_chains = 0;
	UT_ASSERT_EQ(cluster_tt_recovery_physical_rollback(), 0);
	UT_ASSERT_EQ(rollback_reads, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT_EQ(rollback_chains, 0);
}

UT_TEST(test_disabled_legacy_rollback_does_not_scan)
{
	cluster_shared_config = false;
	for (unsigned mode = 0; mode < 3; mode++) {
		cluster_enabled = mode != 0;
		cluster_tt_recovery_resolve_active = mode != 1;
		cluster_node_id = mode == 2 ? -1 : 0;
		rollback_reads = rollback_chains = 0;
		UT_ASSERT_EQ(cluster_tt_recovery_physical_rollback(), 0);
		UT_ASSERT_EQ(rollback_reads, 0);
		UT_ASSERT_EQ(rollback_chains, 0);
	}
}

/* Original startup entries consume immutable full smgr pages. File presence,
 * chain mutation and Lamport publication are the explicit fixture boundaries. */
static void
startup_seed(bool v2, int node)
{
	cluster_enabled = cluster_tt_recovery_resolve_active = true;
	cluster_shared_config = false;
	cluster_node_id = node;
	startup_segment = (uint32)(node + 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE;
	if (v2) {
		UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(
			startup_segment, (uint8)(node + 1), startup_page.data));
	} else {
		cluster_undo_segment_make_header_bytes(
			startup_segment, (uint8)(node + 1), startup_page.data);
	}
	startup_page_present = true;
	rollback_reads = rollback_chains = 0;
	startup_observes = startup_highwater_counts = 0;
	startup_observed_scn = InvalidScn;
	memset(startup_chain_expect, 0, sizeof(startup_chain_expect));
	memset(startup_chain_head, 0, sizeof(startup_chain_head));
}

static UBA
startup_head(unsigned offset)
{
	return uba_encode(startup_segment, offset + 1, offset, 128);
}

UT_TEST(test_startup_v1_highwater_ties_and_rollback_identity_stay_exact)
{
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)startup_page.data;
	PGAlignedBlock before;
	SCN first = UINT64_C(0x0200000000000123);

	startup_seed(false, 1);
	header->tt_slots[0] = (TTSlot){ .xid = 100, .wrap = 2,
		.status = TT_SLOT_COMMITTED, .commit_scn = first };
	header->tt_slots[47] = (TTSlot){ .xid = 147, .wrap = 4,
		.status = TT_SLOT_COMMITTED, .commit_scn = UINT64_C(0x7f00000000000123) };
	header->tt_slots[31] = (TTSlot){ .xid = 131, .wrap = 9,
		.status = TT_SLOT_ABORTED, .first_undo_block = startup_head(31) };
	memcpy(before.data, startup_page.data, BLCKSZ);
	UT_ASSERT_EQ(cluster_tt_recovery_observe_scn_highwater(), 1);
	UT_ASSERT_EQ(startup_observed_scn, first);
	UT_ASSERT_EQ(startup_observes, 1);
	UT_ASSERT_EQ(startup_highwater_counts, 1);
	rollback_reads = 0;
	UT_ASSERT_EQ(cluster_tt_recovery_physical_rollback(), 0);
	UT_ASSERT_EQ(rollback_reads, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT_EQ(rollback_chains, 1);
	UT_ASSERT_EQ(startup_chain_expect[0].origin_node_id, 1);
	UT_ASSERT_EQ(startup_chain_expect[0].tt_slot_segment_id, startup_segment);
	UT_ASSERT_EQ(startup_chain_expect[0].tt_slot_id, cluster_tt_slot_offset_to_id(31));
	UT_ASSERT_EQ(startup_chain_expect[0].tt_wrap_plus1, 10);
	UT_ASSERT_EQ(startup_chain_expect[0].xid, 131);
	UT_ASSERT(memcmp(&startup_chain_head[0], &header->tt_slots[31].first_undo_block, sizeof(UBA)) == 0);
	UT_ASSERT(memcmp(before.data, startup_page.data, BLCKSZ) == 0);
}

UT_TEST(test_startup_tt40_highwater_reads_every_actual_slot_before_publish)
{
	for (uint16 peak = 0; peak < TT_SLOTS_PER_SEGMENT; peak++) {
		UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)startup_page.data;
		PGAlignedBlock before;

		startup_seed(true, SCN_MAX_VALID_NODE_ID);
		cluster_tt_recovery_resolve_active = false; /* independent highwater gate */
		for (uint16 i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
			header->tt_slots[i] = (TTSlotV2){ .xid = 100 + i, .wrap = 5,
				.status = TT_SLOT_COMMITTED, .commit_scn = i == peak ? 900 : 100,
				.original_full_xid = ((uint64)(i % 2) << 32) | (100 + i) };
		memcpy(before.data, startup_page.data, BLCKSZ);
		UT_ASSERT_EQ(cluster_tt_recovery_observe_scn_highwater(), 1);
		UT_ASSERT_EQ(startup_observed_scn, 900);
		UT_ASSERT_EQ(startup_observes, 1);
		UT_ASSERT_EQ(startup_highwater_counts, 1);
		UT_ASSERT_EQ(rollback_reads, CLUSTER_UNDO_SEGS_PER_INSTANCE);
		UT_ASSERT_EQ(rollback_chains, 0);
		UT_ASSERT(memcmp(before.data, startup_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_startup_tt40_rollback_enumerates_original_exact_prefixes)
{
	UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)startup_page.data;
	PGAlignedBlock before;

	startup_seed(true, SCN_MAX_VALID_NODE_ID);
	for (uint16 i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
		header->tt_slots[i] = (TTSlotV2){ .xid = 100 + i, .wrap = i,
			.status = TT_SLOT_ABORTED, .first_undo_block = startup_head(i),
			.original_full_xid = ((uint64)(i % 2) << 32) | (100 + i) };
	memcpy(before.data, startup_page.data, BLCKSZ);
	UT_ASSERT_EQ(cluster_tt_recovery_physical_rollback(), 0);
	UT_ASSERT_EQ(rollback_reads, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT_EQ(rollback_chains, TT_SLOTS_PER_SEGMENT);
	for (uint16 i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		UT_ASSERT_EQ(startup_chain_expect[i].origin_node_id, SCN_MAX_VALID_NODE_ID);
		UT_ASSERT_EQ(startup_chain_expect[i].tt_slot_segment_id, startup_segment);
		UT_ASSERT_EQ(startup_chain_expect[i].tt_slot_id, cluster_tt_slot_offset_to_id(i));
		UT_ASSERT_EQ(startup_chain_expect[i].tt_wrap_plus1, i + 1);
		UT_ASSERT_EQ(startup_chain_expect[i].xid, 100 + i);
		UT_ASSERT(memcmp(&startup_chain_head[i], &header->tt_slots[i].first_undo_block, sizeof(UBA)) == 0);
	}
	UT_ASSERT_EQ(startup_observes, 0);
	UT_ASSERT(memcmp(before.data, startup_page.data, BLCKSZ) == 0);
}

UT_TEST(test_startup_tt40_bad_tail_refuses_before_observe_or_chain)
{
	for (unsigned fault = 0; fault < 17; fault++) {
		for (unsigned scan = 0; scan < 2; scan++) {
			UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)startup_page.data;
			PGAlignedBlock before;
			volatile bool refused = false;

			startup_seed(true, 0);
			header->tt_slots[0] = (TTSlotV2){ .xid = 100, .wrap = 2,
				.status = TT_SLOT_ABORTED, .first_undo_block = startup_head(0),
				.original_full_xid = UINT64_C(0x100000064) };
			header->tt_slots[1] = (TTSlotV2){ .xid = 101, .wrap = 2,
				.status = TT_SLOT_COMMITTED, .commit_scn = 900,
				.original_full_xid = UINT64_C(0x100000065) };
			header->tt_slots[47] = (TTSlotV2){ .xid = 147, .wrap = 2,
				.status = TT_SLOT_COMMITTED, .commit_scn = 100,
				.original_full_xid = UINT64_C(0x100000093) };
			switch (fault) {
				case 0: header->tt_layout_version = 3; break;
				case 1: header->tt_slot_bytes = sizeof(TTSlot); break;
				case 2: header->tt_layout_reserved = 1; break;
				case 3: header->segment_id++; break;
				case 4: header->owner_instance++; break;
				case 5: header->tt_slots_count--; break;
				case 6: header->segment_size_bytes--; break;
				case 7: header->wrap_count = UINT32_MAX; break;
				case 8: header->tt_slots[47].original_full_xid = UINT64_MAX; break;
				case 9: header->tt_slots[47].original_full_xid++; break;
				case 10: header->tt_slots[47].xid = FrozenTransactionId; break;
				case 11: header->tt_slots[47].status = TT_SLOT_RECYCLABLE + 1; break;
				case 12: header->tt_slots[47].flags = ~TT_SLOT_FLAGS_KNOWN; break;
				case 13: header->tt_slots[47].wrap = TT_WRAP_INVALID; break;
				case 14: header->tt_slots[47].commit_scn = UINT64_MAX; break;
				case 15: header->tt_slots[47].status = TT_SLOT_ABORTED; break;
				case 16: header->tt_slots[47].status = TT_SLOT_UNUSED; break;
			}
			memcpy(before.data, startup_page.data, BLCKSZ);
			last_ereport_elevel = last_ereport_errcode = 0;
			PG_TRY();
			{
				if (scan == 0)
					(void)cluster_tt_recovery_observe_scn_highwater();
				else
					(void)cluster_tt_recovery_physical_rollback();
			}
			PG_CATCH();
			{
				refused = true;
			}
			PG_END_TRY();
			UT_ASSERT(refused);
			UT_ASSERT_EQ(last_ereport_elevel, ERROR);
			UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_DATA_CORRUPTED);
			UT_ASSERT_EQ(startup_observes, 0);
			UT_ASSERT_EQ(startup_highwater_counts, 0);
			UT_ASSERT_EQ(rollback_chains, 0);
			UT_ASSERT(memcmp(before.data, startup_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_startup_highwater_disabled_absent_and_nonterminal_stay_readonly)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)startup_page.data;

		startup_seed(true, 0);
		if (mode == 0) cluster_enabled = false;
		if (mode == 1) cluster_node_id = -1;
		if (mode == 2) startup_page_present = false;
		if (mode == 3)
			header->tt_slots[47] = (TTSlotV2){ .xid = 147, .wrap = 5,
				.status = TT_SLOT_ACTIVE, .original_full_xid = UINT64_C(0x100000093) };
		UT_ASSERT_EQ(cluster_tt_recovery_observe_scn_highwater(), 0);
		UT_ASSERT_EQ(rollback_reads, mode < 2 ? 0 : CLUSTER_UNDO_SEGS_PER_INSTANCE);
		UT_ASSERT_EQ(rollback_chains, 0);
		UT_ASSERT_EQ(startup_observes, 0);
		UT_ASSERT_EQ(startup_highwater_counts, 0);
	}
	startup_page_present = false;
}

int
main(int argc, char **argv)
{
	UT_PLAN(8);
	UT_RUN(test_shared_legacy_rollback_refuses_before_any_undo_read);
	UT_RUN(test_nonshared_legacy_rollback_keeps_original_scan);
	UT_RUN(test_disabled_legacy_rollback_does_not_scan);
	UT_RUN(test_startup_v1_highwater_ties_and_rollback_identity_stay_exact);
	UT_RUN(test_startup_tt40_highwater_reads_every_actual_slot_before_publish);
	UT_RUN(test_startup_tt40_rollback_enumerates_original_exact_prefixes);
	UT_RUN(test_startup_tt40_bad_tail_refuses_before_observe_or_chain);
	UT_RUN(test_startup_highwater_disabled_absent_and_nonterminal_stay_readonly);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
