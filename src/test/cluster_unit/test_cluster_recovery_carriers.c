/* Process-local recovery carrier contracts; no native qualification fixture. */
#include "postgres.h"

#include "cluster/cluster_recovery_debt.h"
#include "cluster/cluster_recovery_format.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

UT_TEST(test_file_reset_forgets_only_the_value)
{
	ClusterRecoveryFileIdentityV2 identity = {0};
	ClusterRecoveryFileIdentityV2 saved;
	ClusterRecoveryFileIdentityV2 empty;

	identity.system_identifier = UINT64CONST(0x100000001);
	identity.database_incarnation = 12;
	identity.creation_generation = UINT64CONST(0x200000003);
	identity.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	identity.origin = 0;
	saved = identity;
	memset(&empty, 0, sizeof(empty));
	cluster_recovery_file_identity_reset_v2(&identity);
	UT_ASSERT(memcmp(&identity, &empty, sizeof(identity)) == 0);
	UT_ASSERT_EQ(saved.creation_generation, UINT64CONST(0x200000003));
	cluster_recovery_file_identity_reset_v2(NULL);
}

UT_TEST(test_checkpoint_reset_does_not_destroy_borrowed_inputs)
{
	ClusterRecoveryCheckpointSourceV2 source = {0};
	ClusterRecoveryCheckpointFileV2 file = {0};
	ClusterRecoveryCheckpointCoverageV2 view = {0};
	ClusterRecoveryCheckpointCoverageV2 empty;

	source.post_flush_issued = FullTransactionIdFromU64(UINT64CONST(0x300000041));
	file.identity.creation_generation = 19;
	view.sources = &source;
	view.source_count = 1;
	view.files = &file;
	view.file_count = 1;
	/* An opaque address is a borrow tag here; the test never dereferences it. */
	view.custody = (const ClusterRecoveryCheckpointCustodyV2 *) (const void *) &source;
	memset(&empty, 0, sizeof(empty));
	cluster_recovery_checkpoint_coverage_reset_v2(&view);
	UT_ASSERT(memcmp(&view, &empty, sizeof(view)) == 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(source.post_flush_issued),
				 UINT64CONST(0x300000041));
	UT_ASSERT_EQ(file.identity.creation_generation, 19);
	cluster_recovery_checkpoint_coverage_reset_v2(NULL);
}

UT_TEST(test_debt_reset_does_not_retire_entries_or_cap_batch_at_64)
{
	ClusterRecoveryDebtEntryV2 entries[65] = {{0}};
	ClusterRecoveryDebtBatchV2 view = {0};
	ClusterRecoveryDebtBatchV2 empty;

	entries[64].ordinal = 64;
	entries[64].top_full_xid = FullTransactionIdFromU64(UINT64CONST(0x400000063));
	entries[64].owner = CLUSTER_RECOVERY_DEBT_OWNER_RELMAP_V2;
	view.state = CLUSTER_RECOVERY_DEBT_PREPARED_V2;
	view.total_entry_count = 65;
	view.total_page_count = 3;
	view.page.index = 2;
	view.page.first_entry = 64;
	view.page.entry_count = 1;
	view.page.entries = &entries[64];
	view.custody = (const ClusterRecoveryDebtCustodyV2 *) (const void *) entries;
	memset(&empty, 0, sizeof(empty));
	UT_ASSERT_EQ(view.total_entry_count, 65);
	UT_ASSERT_EQ(U64FromFullTransactionId(view.page.entries[0].top_full_xid),
				 UINT64CONST(0x400000063));
	cluster_recovery_debt_batch_reset_v2(&view);
	UT_ASSERT(memcmp(&view, &empty, sizeof(view)) == 0);
	UT_ASSERT_EQ(entries[64].ordinal, 64);
	UT_ASSERT_EQ(entries[64].owner, CLUSTER_RECOVERY_DEBT_OWNER_RELMAP_V2);
	UT_ASSERT_EQ(U64FromFullTransactionId(entries[64].top_full_xid),
				 UINT64CONST(0x400000063));
	cluster_recovery_debt_batch_reset_v2(NULL);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_file_reset_forgets_only_the_value);
	UT_RUN(test_checkpoint_reset_does_not_destroy_borrowed_inputs);
	UT_RUN(test_debt_reset_does_not_retire_entries_or_cap_batch_at_64);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
