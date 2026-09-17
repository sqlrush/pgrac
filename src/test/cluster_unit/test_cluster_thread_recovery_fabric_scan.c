/*-------------------------------------------------------------------------
 * test_cluster_thread_recovery_fabric_scan.c
 *    Exact ROOT-cut WAL scan into one immutable recovery fabric.
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_thread_recovery_authority.h"
#include "cluster/cluster_thread_recovery_fabric.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static char fabric_object;
static char reader_private_object;
static char retention_pin_object;
static XLogReaderState reader;
static DecodedXLogRecord decoded[2];
static XLogRecord raw_records[2];
static XLogRecPtr record_begin[2];
static XLogRecPtr record_end[2];
static int record_count;
static int read_index;
static int feed_fail_at;
static int authority_revalidations;
static int reader_make_count;
static int reader_free_count;
static int plan_create_count;
static int plan_feed_count;
static int plan_seal_count;
static int plan_destroy_count;
static XLogRecPtr begin_read_lsn;
static bool authority_current;

ClusterThreadRecoveryAuthorityResultV1
cluster_thread_recovery_authority_revalidate_nowait_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority)
{
	authority_revalidations++;
	return authority != NULL && authority_current ? CLUSTER_THREAD_AUTHORITY_OK
												  : CLUSTER_THREAD_AUTHORITY_ROOT_STALE;
}

bool
cluster_thread_recovery_authority_covers_window_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority, uint16 dead_thread, XLogRecPtr scan_begin,
	XLogRecPtr scan_end)
{
	return authority != NULL && dead_thread == 2 && scan_begin == 0x100 && scan_end == 0x200;
}

XLogReaderState *
cluster_thread_wal_reader_make(uint16 dead_thread, void **private_out)
{
	UT_ASSERT_EQ(dead_thread, 2);
	UT_ASSERT(private_out != NULL);
	reader_make_count++;
	memset(&reader, 0, sizeof(reader));
	reader.seg.ws_tli = 1;
	*private_out = &reader_private_object;
	return &reader;
}

void
cluster_thread_wal_reader_free(XLogReaderState *state, void *private_state)
{
	UT_ASSERT(state == &reader && private_state == &reader_private_object);
	reader_free_count++;
}

void
XLogBeginRead(XLogReaderState *state, XLogRecPtr rec_ptr)
{
	UT_ASSERT(state == &reader);
	begin_read_lsn = rec_ptr;
	read_index = 0;
}

XLogRecord *
XLogReadRecord(XLogReaderState *state, char **error_message)
{
	int index;

	UT_ASSERT(state == &reader && error_message != NULL);
	*error_message = NULL;
	if (read_index >= record_count)
		return NULL;
	index = read_index++;
	state->ReadRecPtr = record_begin[index];
	state->EndRecPtr = record_end[index];
	state->record = &decoded[index];
	return &raw_records[index];
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_plan_create_v1(
	const ClusterThreadRecoveryFabricPlanRequestV1 *request,
	ClusterThreadRecoveryFabricPlanV1 **out_plan)
{
	const RfContributorStreamCutV1 *cut;

	UT_ASSERT(request != NULL && out_plan != NULL);
	UT_ASSERT_EQ(request->system_identifier, 99);
	UT_ASSERT(request->storage_uuid[0] == 3 && request->participant_count == 1
			  && request->retention_binding_cookie != 0 && !request->space_active);
	cut = request->physical_cuts;
	UT_ASSERT(cut != NULL && cut->failed_thread == 2 && cut->flags == RF_CONTRIBUTOR_CUT_COMPLETE
			  && cut->timeline_id == 7 && cut->scan_begin_inclusive == 0x100
			  && cut->scan_end_exclusive == 0x200);
	plan_create_count++;
	*out_plan = (ClusterThreadRecoveryFabricPlanV1 *)&fabric_object;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_plan_feed_record_v1(ClusterThreadRecoveryFabricPlanV1 *plan,
												   XLogReaderState *state, uint16 participant_index)
{
	UT_ASSERT(plan == (ClusterThreadRecoveryFabricPlanV1 *)&fabric_object && state == &reader
			  && state->seg.ws_tli == 7 && participant_index == 0);
	plan_feed_count++;
	return plan_feed_count == feed_fail_at ? RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED
										   : RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_plan_seal_v1(ClusterThreadRecoveryFabricPlanV1 *plan)
{
	UT_ASSERT(plan == (ClusterThreadRecoveryFabricPlanV1 *)&fabric_object);
	plan_seal_count++;
	return RF_PAGE_PROOF_DETAIL_OK;
}

void
cluster_thread_recovery_fabric_plan_destroy_v1(ClusterThreadRecoveryFabricPlanV1 **plan)
{
	UT_ASSERT(plan != NULL && *plan == (ClusterThreadRecoveryFabricPlanV1 *)&fabric_object);
	plan_destroy_count++;
	*plan = NULL;
}

static void
init_case(ClusterThreadRecoveryAuthorityV1 *authority)
{
	static ClusterRecoveryDutyKey duty;
	static ClusterControlRootSnapshot root;

	memset(authority, 0, sizeof(*authority));
	memset(&duty, 0, sizeof(duty));
	memset(&root, 0, sizeof(root));
	duty.system_identifier = 99;
	memset(duty.storage_uuid, 3, sizeof(duty.storage_uuid));
	duty.origin_thread_id = 2;
	root.identity = duty;
	root.checkpoint_tli = 7;
	root.tail_tli = 7;
	root.checkpoint_lower_lsn = 0x100;
	root.validated_tail_lsn_exclusive = 0x200;
	authority->duty = &duty;
	authority->root_snapshot = &root;
	authority->retention_pin = (ClusterWalRetentionPin *)&retention_pin_object;
	record_begin[0] = 0x100;
	record_end[0] = 0x140;
	record_begin[1] = 0x140;
	record_end[1] = 0x200;
	record_count = 2;
	read_index = 0;
	feed_fail_at = 0;
	authority_revalidations = reader_make_count = reader_free_count = 0;
	plan_create_count = plan_feed_count = plan_seal_count = 0;
	plan_destroy_count = 0;
	begin_read_lsn = InvalidXLogRecPtr;
	authority_current = true;
}

UT_TEST(test_scans_exact_root_cut_and_seals_only_at_upper_boundary)
{
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	uint64 records = 0;

	init_case(&authority);
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_scan_root_v1(2, 0x100, 0x200, &authority, false,
															 &plan, &records),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(plan == (ClusterThreadRecoveryFabricPlanV1 *)&fabric_object);
	UT_ASSERT_EQ(records, 2);
	UT_ASSERT_EQ(begin_read_lsn, 0x100);
	UT_ASSERT(authority_revalidations >= 2);
	UT_ASSERT_EQ(reader_make_count, 1);
	UT_ASSERT_EQ(reader_free_count, 1);
	UT_ASSERT_EQ(plan_create_count, 1);
	UT_ASSERT_EQ(plan_feed_count, 2);
	UT_ASSERT_EQ(plan_seal_count, 1);
	UT_ASSERT_EQ(plan_destroy_count, 0);
}

UT_TEST(test_early_end_destroys_unsealed_plan)
{
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	uint64 records = 9;

	init_case(&authority);
	record_count = 1;
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_scan_root_v1(2, 0x100, 0x200, &authority, false,
															 &plan, &records),
				 RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	UT_ASSERT(plan == NULL && records == 0);
	UT_ASSERT_EQ(plan_seal_count, 0);
	UT_ASSERT_EQ(plan_destroy_count, 1);
	UT_ASSERT_EQ(reader_free_count, 1);
}

UT_TEST(test_feed_failure_poisons_and_destroys_whole_plan)
{
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	uint64 records = 9;

	init_case(&authority);
	feed_fail_at = 2;
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_scan_root_v1(2, 0x100, 0x200, &authority, false,
															 &plan, &records),
				 RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED);
	UT_ASSERT(plan == NULL && records == 0);
	UT_ASSERT_EQ(plan_feed_count, 2);
	UT_ASSERT_EQ(plan_seal_count, 0);
	UT_ASSERT_EQ(plan_destroy_count, 1);
}

UT_TEST(test_non_root_window_is_rejected_before_reader_or_plan)
{
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	uint64 records = 9;

	init_case(&authority);
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_scan_root_v1(2, 0x120, 0x200, &authority, false,
															 &plan, &records),
				 RF_PAGE_PROOF_DETAIL_ROOT_STALE);
	UT_ASSERT(plan == NULL && records == 0);
	UT_ASSERT_EQ(reader_make_count, 0);
	UT_ASSERT_EQ(plan_create_count, 0);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_scans_exact_root_cut_and_seals_only_at_upper_boundary);
	UT_RUN(test_early_end_destroys_unsealed_plan);
	UT_RUN(test_feed_failure_poisons_and_destroys_whole_plan);
	UT_RUN(test_non_root_window_is_rejected_before_reader_or_plan);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
