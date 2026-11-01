/* Author: SqlRush <sqlrush@gmail.com> */
/* Execute the original legacy rollback entry. Undo reads and chain mutation
 * are explicit boundaries; shared refusal must precede both. */
int durable_fixture_main(int argc, char **argv);
struct ClusterResId;
#define main durable_fixture_main
#include "test_cluster_tt_durable.c"
#undef main

bool cluster_enabled = true;
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

/* The real post-EOR scanner and liveness wrapper, with only their IO and
 * native CLOG/process dependencies supplied by the existing fixture. */
static bool resolve_committed, resolve_in_progress;
static unsigned resolve_clog_reads, resolve_proc_reads, resolve_aborts;
static unsigned resolve_probe_calls;
static ClusterUndoSmgrFinalState resolve_probe_state;
static bool resolve_probe_identity_ok;
static uint32 resolve_extra_segment;
static PGAlignedBlock resolve_extra_block;
static ClusterUndoSmgrFinalState resolve_extra_state;
static SCN highwater_observed;
static unsigned highwater_observe_calls, highwater_count_calls, highwater_probes_at_observe;

/* Explicit IO boundary for the original scanner. The separate smgr publication
 * tests execute the real final-path probe, including size/identity failures. */
ClusterUndoSmgrFinalState
cluster_undo_smgr_probe_segment(ClusterUndoPathIntent intent, uint32 segment_id, uint8 owner,
								char block0[BLCKSZ])
{
	resolve_probe_calls++;
	if (owner != (uint8)(cluster_node_id + 1) || intent != cluster_undo_intent_for_owner(owner))
		resolve_probe_identity_ok = false;
	if (segment_id == resolve_extra_segment) {
		if (resolve_extra_state == CLUSTER_UNDO_SMGR_FINAL_EXACT)
			memcpy(block0, resolve_extra_block.data, BLCKSZ);
		return resolve_extra_state;
	}
	if (segment_id != g_canned_block_segment)
		return CLUSTER_UNDO_SMGR_FINAL_ABSENT;
	if (resolve_probe_state == CLUSTER_UNDO_SMGR_FINAL_EXACT)
		memcpy(block0, g_canned_block, BLCKSZ);
	return resolve_probe_state;
}

bool
TransactionIdDidCommit(TransactionId xid)
{
	resolve_clog_reads++;
	return resolve_committed;
}

bool TransactionIdIsInProgress(TransactionId xid);
bool
TransactionIdIsInProgress(TransactionId xid)
{
	resolve_proc_reads++;
	return resolve_in_progress;
}

void
cluster_tt_recovery_count_active_resolved_aborted(void)
{
	resolve_aborts++;
}

void
cluster_scn_recovery_replay_observe(SCN scn)
{
	highwater_observed = scn;
	highwater_observe_calls++;
	highwater_probes_at_observe = resolve_probe_calls;
}

void
cluster_tt_recovery_count_scn_highwater_recovered(void)
{
	highwater_count_calls++;
}

#include "test_cluster_tt_resolve_entry.inc"

static void
resolve_setup(bool shared, TransactionId xid)
{
	cluster_enabled = cluster_tt_recovery_resolve_active = true;
	cluster_shared_config = shared;
	cluster_node_id = 0;
	resolve_committed = resolve_in_progress = false;
	resolve_clog_reads = resolve_proc_reads = resolve_aborts = 0;
	resolve_probe_calls = 0;
	resolve_probe_state = CLUSTER_UNDO_SMGR_FINAL_EXACT;
	resolve_probe_identity_ok = true;
	resolve_extra_segment = 0;
	resolve_extra_state = CLUSTER_UNDO_SMGR_FINAL_EXACT;
	memset(resolve_extra_block.data, 0, BLCKSZ);
	highwater_observed = InvalidScn;
	highwater_observe_calls = highwater_count_calls = highwater_probes_at_observe = 0;
	g_canned_block_segment = 1;
	g_read_block_ok = g_read_hdr_ok = true;
	g_read_hdr_fail_on_call = 0;
	g_read_hdr_second_enabled = false;
	g_read_block_calls = g_read_hdr_calls = g_write_hdr_calls = g_tt_wal_count = 0;
	last_ereport_elevel = last_ereport_errcode = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(7, TT_SLOT_ACTIVE, xid, InvalidScn);
	g_canned_slot = ((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7];
	g_canned_slot.first_undo_block.raw[0] = 19;
	g_canned_slot.first_undo_block.raw[1] = 37;
	((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7] = g_canned_slot;
}

UT_TEST(test_shared_active_never_uses_local_liveness_or_writes_abort)
{
	/* Neither a local committed/process answer nor invalid raw xid proves
	 * the exact shared top/base/terminal obligation or permits clearing head. */
	for (unsigned mode = 0; mode < 5; mode++) {
		volatile bool refused = false;
		PGAlignedBlock before;
		resolve_setup(true, mode == 3	? InvalidTransactionId
							: mode == 4 ? FrozenTransactionId
										: 777);
		resolve_committed = mode == 1;
		resolve_in_progress = mode == 2;
		memcpy(before.data, g_canned_block, BLCKSZ);
		PG_TRY();
		{
			(void)cluster_tt_recovery_resolve_active_slots();
		}
		PG_CATCH();
		{
			refused = true;
		}
		PG_END_TRY();
		cluster_shared_config = false;
		UT_ASSERT(refused);
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(resolve_clog_reads, 0);
		UT_ASSERT_EQ(resolve_proc_reads, 0);
		UT_ASSERT_EQ(resolve_aborts, 0);
		UT_ASSERT_EQ(resolve_probe_calls, 1);
		UT_ASSERT(resolve_probe_identity_ok);
		UT_ASSERT_EQ(g_read_block_calls, 0);
		UT_ASSERT_EQ(g_tt_wal_count, 0);
		UT_ASSERT_EQ(g_read_hdr_calls, 0);
		UT_ASSERT_EQ(g_write_hdr_calls, 0);
		UT_ASSERT_EQ(memcmp(before.data, g_canned_block, BLCKSZ), 0);
	}
}

UT_TEST(test_shared_terminal_only_scan_preserves_slots)
{
	PGAlignedBlock before;
	resolve_setup(true, 777);
	seed_block_slot(7, TT_SLOT_COMMITTED, 777, 123);
	seed_block_slot(8, TT_SLOT_ABORTED, 778, InvalidScn);
	memcpy(before.data, g_canned_block, BLCKSZ);
	UT_ASSERT_EQ(cluster_tt_recovery_resolve_active_slots(), 0);
	cluster_shared_config = false;
	UT_ASSERT_EQ(resolve_clog_reads + resolve_proc_reads + resolve_aborts, 0);
	UT_ASSERT_EQ(g_tt_wal_count + g_write_hdr_calls, 0);
	UT_ASSERT_EQ(memcmp(before.data, g_canned_block, BLCKSZ), 0);
}

UT_TEST(test_native_active_resolution_keeps_liveness_and_original_writer)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		resolve_setup(false, 777);
		resolve_committed = mode == 1;
		resolve_in_progress = mode == 2;
		UT_ASSERT_EQ(cluster_tt_recovery_resolve_active_slots(), mode == 0 ? 1 : 0);
		UT_ASSERT_EQ(resolve_clog_reads, 1);
		UT_ASSERT_EQ(resolve_proc_reads, 1);
		UT_ASSERT_EQ(resolve_aborts, mode == 0 ? 1 : 0);
		UT_ASSERT_EQ(g_tt_wal_count, mode == 0 ? 1 : 0);
		UT_ASSERT_EQ(g_write_hdr_calls, mode == 0 ? 1 : 0);
		UT_ASSERT_EQ(resolve_probe_calls, 0);
		if (mode == 0)
			UT_ASSERT_EQ(g_last_written_slot.status, TT_SLOT_ABORTED);
	}
}

UT_TEST(test_disabled_active_scanner_keeps_original_noop)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		resolve_setup(true, 777);
		cluster_enabled = mode != 0;
		cluster_tt_recovery_resolve_active = mode != 1;
		cluster_node_id = mode == 2 ? -1 : 0;
		UT_ASSERT_EQ(cluster_tt_recovery_resolve_active_slots(), 0);
		cluster_shared_config = false;
		UT_ASSERT_EQ(g_read_block_calls + g_read_hdr_calls + g_write_hdr_calls, 0);
		UT_ASSERT_EQ(resolve_clog_reads + resolve_proc_reads + resolve_aborts, 0);
		UT_ASSERT_EQ(resolve_probe_calls, 0);
	}
}

UT_TEST(test_shared_present_invalid_or_unreadable_segment_refuses)
{
	const ClusterUndoSmgrFinalState failures[]
		= { CLUSTER_UNDO_SMGR_FINAL_INVALID, CLUSTER_UNDO_SMGR_FINAL_IO_ERROR,
			(ClusterUndoSmgrFinalState)99 };
	for (unsigned mode = 0; mode < lengthof(failures); mode++) {
		volatile bool refused = false;
		PGAlignedBlock before;
		resolve_setup(true, 777);
		resolve_probe_state = failures[mode];
		g_read_block_ok = false;
		memcpy(before.data, g_canned_block, BLCKSZ);
		PG_TRY();
		{
			(void)cluster_tt_recovery_resolve_active_slots();
		}
		PG_CATCH();
		{
			refused = true;
		}
		PG_END_TRY();
		cluster_shared_config = false;
		UT_ASSERT(refused);
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(resolve_probe_calls, 1);
		UT_ASSERT(resolve_probe_identity_ok);
		UT_ASSERT_EQ(g_read_block_calls + g_read_hdr_calls, 0);
		UT_ASSERT_EQ(resolve_clog_reads + resolve_proc_reads + resolve_aborts, 0);
		UT_ASSERT_EQ(g_tt_wal_count + g_write_hdr_calls, 0);
		UT_ASSERT_EQ(memcmp(before.data, g_canned_block, BLCKSZ), 0);
	}
}

UT_TEST(test_shared_scan_keeps_noncurrent_generation_zero_unreleased_slots)
{
	PGAlignedBlock before;
	resolve_setup(true, 777);
	g_canned_block_segment = CLUSTER_UNDO_SEGS_PER_INSTANCE;
	seed_block_slot(7, TT_SLOT_COMMITTED, 777, 123);
	seed_block_slot(TT_SLOTS_PER_SEGMENT - 1, TT_SLOT_ABORTED, 778, InvalidScn);
	memcpy(before.data, g_canned_block, BLCKSZ);
	UT_ASSERT_EQ(cluster_tt_recovery_resolve_active_slots(), 0);
	cluster_shared_config = false;
	UT_ASSERT_EQ(resolve_probe_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT(resolve_probe_identity_ok);
	UT_ASSERT_EQ(g_read_block_calls + g_read_hdr_calls, 0);
	UT_ASSERT_EQ(resolve_clog_reads + resolve_proc_reads + resolve_aborts, 0);
	UT_ASSERT_EQ(g_tt_wal_count + g_write_hdr_calls, 0);
	UT_ASSERT_EQ(memcmp(before.data, g_canned_block, BLCKSZ), 0);
}

UT_TEST(test_shared_scan_refuses_active_last_slot_in_noncurrent_generation_zero_segment)
{
	volatile bool refused = false;
	PGAlignedBlock before;
	resolve_setup(true, 777);
	g_canned_block_segment = CLUSTER_UNDO_SEGS_PER_INSTANCE;
	memset(g_canned_block, 0, BLCKSZ);
	seed_block_slot(TT_SLOTS_PER_SEGMENT - 1, TT_SLOT_ACTIVE, 777, InvalidScn);
	((UndoSegmentHeaderData *)g_canned_block)->tt_slots[TT_SLOTS_PER_SEGMENT - 1].first_undo_block
		= g_canned_slot.first_undo_block;
	memcpy(before.data, g_canned_block, BLCKSZ);
	PG_TRY();
	{
		(void)cluster_tt_recovery_resolve_active_slots();
	}
	PG_CATCH();
	{
		refused = true;
	}
	PG_END_TRY();
	cluster_shared_config = false;
	UT_ASSERT(refused);
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(resolve_probe_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT(resolve_probe_identity_ok);
	UT_ASSERT_EQ(g_read_block_calls + g_read_hdr_calls, 0);
	UT_ASSERT_EQ(resolve_clog_reads + resolve_proc_reads + resolve_aborts, 0);
	UT_ASSERT_EQ(g_tt_wal_count + g_write_hdr_calls, 0);
	UT_ASSERT_EQ(memcmp(before.data, g_canned_block, BLCKSZ), 0);
}

UT_TEST(test_shared_scan_skips_only_absent_segments)
{
	resolve_setup(true, 777);
	resolve_probe_state = CLUSTER_UNDO_SMGR_FINAL_ABSENT;
	g_read_block_ok = false;
	UT_ASSERT_EQ(cluster_tt_recovery_resolve_active_slots(), 0);
	cluster_shared_config = false;
	UT_ASSERT_EQ(resolve_probe_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT(resolve_probe_identity_ok);
	UT_ASSERT_EQ(g_read_block_calls + g_read_hdr_calls, 0);
	UT_ASSERT_EQ(resolve_clog_reads + resolve_proc_reads + resolve_aborts, 0);
	UT_ASSERT_EQ(g_tt_wal_count + g_write_hdr_calls, 0);
}

UT_TEST(test_shared_highwater_refuses_bad_input_before_observing_any_peak)
{
	const ClusterUndoSmgrFinalState failures[]
		= { CLUSTER_UNDO_SMGR_FINAL_INVALID, CLUSTER_UNDO_SMGR_FINAL_IO_ERROR,
			(ClusterUndoSmgrFinalState)99 };
	for (unsigned mode = 0; mode < 2 * lengthof(failures); mode++) {
		volatile bool refused = false;
		PGAlignedBlock before;
		bool late = mode >= lengthof(failures);
		resolve_setup(true, 777);
		seed_block_slot(7, TT_SLOT_COMMITTED, 777, 123);
		if (late) {
			resolve_extra_segment = CLUSTER_UNDO_SEGS_PER_INSTANCE;
			resolve_extra_state = failures[mode % lengthof(failures)];
		} else
			resolve_probe_state = failures[mode];
		memcpy(before.data, g_canned_block, BLCKSZ);
		PG_TRY();
		{
			(void)cluster_tt_recovery_observe_scn_highwater();
		}
		PG_CATCH();
		{
			refused = true;
		}
		PG_END_TRY();
		cluster_shared_config = false;
		UT_ASSERT(refused);
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(resolve_probe_calls, late ? CLUSTER_UNDO_SEGS_PER_INSTANCE : 1);
		UT_ASSERT(resolve_probe_identity_ok);
		UT_ASSERT_EQ(highwater_observe_calls + highwater_count_calls, 0);
		UT_ASSERT_EQ(g_read_block_calls + g_read_hdr_calls + g_write_hdr_calls, 0);
		UT_ASSERT_EQ(g_tt_wal_count + resolve_clog_reads + resolve_proc_reads + resolve_aborts, 0);
		UT_ASSERT_EQ(memcmp(before.data, g_canned_block, BLCKSZ), 0);
	}
}

UT_TEST(test_shared_highwater_observes_last_slot_after_all_owned_segments)
{
	PGAlignedBlock before, extra_before;
	UndoSegmentHeaderData *extra;
	resolve_setup(true, 777);
	cluster_tt_recovery_resolve_active = false; /* Highwater has its own startup obligation. */
	cluster_node_id = 3;
	g_canned_block_segment = 3 * CLUSTER_UNDO_SEGS_PER_INSTANCE + 42;
	seed_block_slot(7, TT_SLOT_COMMITTED, 777, 123);
	seed_block_slot(8, TT_SLOT_ACTIVE, 778, 999);
	seed_block_slot(9, TT_SLOT_ABORTED, 779, 999);
	seed_block_slot(10, TT_SLOT_COMMITTED, 780, InvalidScn);
	resolve_extra_segment = 4 * CLUSTER_UNDO_SEGS_PER_INSTANCE;
	extra = (UndoSegmentHeaderData *)resolve_extra_block.data;
	extra->tt_slots[TT_SLOTS_PER_SEGMENT - 1].status = TT_SLOT_COMMITTED;
	extra->tt_slots[TT_SLOTS_PER_SEGMENT - 1].xid = 781;
	extra->tt_slots[TT_SLOTS_PER_SEGMENT - 1].commit_scn = 987;
	memcpy(before.data, g_canned_block, BLCKSZ);
	memcpy(extra_before.data, resolve_extra_block.data, BLCKSZ);
	UT_ASSERT_EQ(cluster_tt_recovery_observe_scn_highwater(), 1);
	cluster_shared_config = false;
	UT_ASSERT_EQ(resolve_probe_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT(resolve_probe_identity_ok);
	UT_ASSERT_EQ(highwater_observe_calls, 1);
	UT_ASSERT_EQ(highwater_count_calls, 1);
	UT_ASSERT_EQ(highwater_observed, 987);
	UT_ASSERT_EQ(highwater_probes_at_observe, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT_EQ(g_read_block_calls + g_read_hdr_calls + g_write_hdr_calls + g_tt_wal_count, 0);
	UT_ASSERT_EQ(memcmp(before.data, g_canned_block, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(extra_before.data, resolve_extra_block.data, BLCKSZ), 0);
}

UT_TEST(test_shared_highwater_absence_or_no_committed_peak_observes_nothing)
{
	for (unsigned absent = 0; absent < 2; absent++) {
		resolve_setup(true, 777);
		resolve_probe_state
			= absent ? CLUSTER_UNDO_SMGR_FINAL_ABSENT : CLUSTER_UNDO_SMGR_FINAL_EXACT;
		g_read_block_ok = false;
		UT_ASSERT_EQ(cluster_tt_recovery_observe_scn_highwater(), 0);
		cluster_shared_config = false;
		UT_ASSERT_EQ(resolve_probe_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);
		UT_ASSERT_EQ(g_read_block_calls, 0);
		UT_ASSERT_EQ(highwater_observe_calls + highwater_count_calls, 0);
	}
}

UT_TEST(test_highwater_keeps_nonshared_reads_and_disabled_noop)
{
	resolve_setup(false, 777);
	seed_block_slot(7, TT_SLOT_COMMITTED, 777, 123);
	UT_ASSERT_EQ(cluster_tt_recovery_observe_scn_highwater(), 1);
	UT_ASSERT_EQ(g_read_block_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT_EQ(resolve_probe_calls, 0);
	UT_ASSERT_EQ(highwater_observed, 123);
	UT_ASSERT_EQ(highwater_observe_calls, 1);
	UT_ASSERT_EQ(highwater_count_calls, 1);
	for (unsigned mode = 0; mode < 2; mode++) {
		resolve_setup(true, 777);
		cluster_enabled = mode != 0;
		cluster_node_id = mode == 1 ? -1 : 0;
		UT_ASSERT_EQ(cluster_tt_recovery_observe_scn_highwater(), 0);
		cluster_shared_config = false;
		UT_ASSERT_EQ(g_read_block_calls + resolve_probe_calls, 0);
		UT_ASSERT_EQ(highwater_observe_calls + highwater_count_calls, 0);
	}
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
	UT_PLAN(15);
	UT_RUN(test_shared_legacy_rollback_refuses_before_any_undo_read);
	UT_RUN(test_nonshared_legacy_rollback_keeps_original_scan);
	UT_RUN(test_disabled_legacy_rollback_does_not_scan);
	UT_RUN(test_shared_active_never_uses_local_liveness_or_writes_abort);
	UT_RUN(test_shared_terminal_only_scan_preserves_slots);
	UT_RUN(test_native_active_resolution_keeps_liveness_and_original_writer);
	UT_RUN(test_disabled_active_scanner_keeps_original_noop);
	UT_RUN(test_shared_present_invalid_or_unreadable_segment_refuses);
	UT_RUN(test_shared_scan_keeps_noncurrent_generation_zero_unreleased_slots);
	UT_RUN(test_shared_scan_refuses_active_last_slot_in_noncurrent_generation_zero_segment);
	UT_RUN(test_shared_scan_skips_only_absent_segments);
	UT_RUN(test_shared_highwater_refuses_bad_input_before_observing_any_peak);
	UT_RUN(test_shared_highwater_observes_last_slot_after_all_owned_segments);
	UT_RUN(test_shared_highwater_absence_or_no_committed_peak_observes_nothing);
	UT_RUN(test_highwater_keeps_nonshared_reads_and_disabled_noop);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
