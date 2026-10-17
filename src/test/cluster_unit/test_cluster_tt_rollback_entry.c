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
struct TTRevertExpect;

static bool
rollback_read(ClusterUndoPathIntent intent, uint32 segment, uint8 owner, BlockNumber block,
			  void *buffer)
{
	rollback_reads++;
	return false;
}

static int
rollback_chain(UBA head, const struct TTRevertExpect *expected)
{
	rollback_chains++;
	return 0;
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

/* Explicit IO boundary for the original scanner. The separate smgr publication
 * tests execute the real final-path probe, including size/identity failures. */
ClusterUndoSmgrFinalState
cluster_undo_smgr_probe_segment(ClusterUndoPathIntent intent, uint32 segment_id, uint8 owner,
								char block0[BLCKSZ])
{
	resolve_probe_calls++;
	if (owner != (uint8)(cluster_node_id + 1) || intent != cluster_undo_intent_for_owner(owner))
		resolve_probe_identity_ok = false;
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

int
main(int argc, char **argv)
{
	UT_PLAN(11);
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
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
