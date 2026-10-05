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
struct TTRevertExpect;

static bool
rollback_read(ClusterUndoPathIntent intent, uint32 segment, uint8 owner,
			  BlockNumber block, void *buffer)
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
	UT_PLAN(3);
	UT_RUN(test_shared_legacy_rollback_refuses_before_any_undo_read);
	UT_RUN(test_nonshared_legacy_rollback_keeps_original_scan);
	UT_RUN(test_disabled_legacy_rollback_does_not_scan);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
