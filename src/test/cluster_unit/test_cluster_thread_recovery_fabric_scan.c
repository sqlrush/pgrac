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
#include "cluster/cluster_wal_tail.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_shared_config;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	if (PG_exception_stack == NULL)
		abort();
	siglongjmp(*PG_exception_stack, 1);
}

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
static int exact_source_count;
static int exact_source_fault;
static int database_bind_count;
static uint32 expected_participants = 1;
static RfContributorStreamCutV1 requested_cuts[2];
static uint16 fault_origin;
static uint16 stale_origin;
static bool expire_during_seal;

ClusterThreadRecoveryAuthorityResultV1
cluster_thread_recovery_authority_revalidate_nowait_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority)
{
	authority_revalidations++;
	return authority != NULL && authority_current
		&& authority->duty->origin_thread_id != stale_origin ? CLUSTER_THREAD_AUTHORITY_OK
												  : CLUSTER_THREAD_AUTHORITY_ROOT_STALE;
}

bool
cluster_thread_recovery_authority_covers_window_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority, uint16 dead_thread, XLogRecPtr scan_begin,
	XLogRecPtr scan_end)
{
	return authority != NULL && dead_thread == authority->duty->origin_thread_id
		&& scan_begin == 0x100 && scan_end == 0x200;
}

XLogReaderState *
cluster_thread_wal_reader_make(uint16 dead_thread, void **private_out)
{
	UT_ASSERT_EQ(dead_thread, 2);
	UT_ASSERT(private_out != NULL);
	reader_make_count++;
	/* The actual legacy factory refuses PRE2: a tid is not a generation. */
	if (cluster_shared_config) {
		*private_out = NULL;
		return NULL;
	}
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

/* Routing boundary only; real files and the exact production root scanner
 * are exercised by test_cluster_control_root/test_cluster_thread_tail. */
ClusterControlRootResult
cluster_control_root_recovery_visit(const ClusterControlRootSnapshot *expected,
									const ClusterControlRootReadToken *token,
									ClusterWalRecordVisitor visitor, void *arg,
									ClusterWalTailObservation *out)
{
	char *error = NULL;
	bool inject = fault_origin == 0 || expected->identity.origin_thread_id == fault_origin;
	UT_ASSERT(expected != NULL && token != NULL && visitor != NULL);
	exact_source_count++;
	memset(out, 0, sizeof(*out));
	memset(&reader, 0, sizeof(reader));
	reader.seg.ws_tli = expected->tail_tli;
	XLogBeginRead(&reader, 0x100);
	while (XLogReadRecord(&reader, &error) != NULL) {
		if (inject && exact_source_fault == 1 && read_index == 2)
			authority_current = false;
		if (inject && exact_source_fault == 3 && read_index == 2)
			pg_re_throw();
		if (inject && exact_source_fault == 5 && read_index == 2)
			stale_origin = 2; /* The earlier source expired while the later one is read. */
		if (!visitor(&reader, arg))
			return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	}
	if (inject && exact_source_fault == 2)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	out->records = record_count;
	out->complete_end = record_end[record_count - 1];
	out->database_incarnation = inject && exact_source_fault == 4 ? 43 : 42;
	if (inject && exact_source_fault == 6)
		out->complete_end--;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_plan_create_v1(
	const ClusterThreadRecoveryFabricPlanRequestV1 *request,
	ClusterThreadRecoveryFabricPlanV1 **out_plan)
{
	const RfContributorStreamCutV1 *cut;

	UT_ASSERT(request != NULL && out_plan != NULL);
	UT_ASSERT_EQ(request->system_identifier, 99);
	UT_ASSERT(request->storage_uuid[0] == 3 && request->participant_count == expected_participants
			  && request->retention_binding_cookie != 0 && !request->space_active);
	cut = request->physical_cuts;
	UT_ASSERT(cut != NULL && cut->failed_thread == 2 && cut->flags == RF_CONTRIBUTOR_CUT_COMPLETE
			  && cut->timeline_id == 7 && cut->scan_begin_inclusive == 0x100
			  && cut->scan_end_exclusive == 0x200);
	memcpy(requested_cuts, cut, expected_participants * sizeof(*cut));
	if (expected_participants == 2) {
		UT_ASSERT_EQ(cut[1].failed_thread, 4);
		UT_ASSERT_EQ(cut[1].timeline_id, 8);
		UT_ASSERT_EQ(cut[1].scan_begin_inclusive, 0x100);
		UT_ASSERT_EQ(cut[1].scan_end_exclusive, 0x200);
	}
	plan_create_count++;
	*out_plan = (ClusterThreadRecoveryFabricPlanV1 *)&fabric_object;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_plan_feed_record_v1(ClusterThreadRecoveryFabricPlanV1 *plan,
												   XLogReaderState *state, uint16 participant_index)
{
	UT_ASSERT(plan == (ClusterThreadRecoveryFabricPlanV1 *)&fabric_object && state == &reader);
	UT_ASSERT_EQ(participant_index, cluster_shared_config ? exact_source_count - 1 : 0);
	UT_ASSERT_EQ(state->seg.ws_tli, requested_cuts[participant_index].timeline_id);
	plan_feed_count++;
	return plan_feed_count == feed_fail_at ? RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED
										   : RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_plan_seal_v1(ClusterThreadRecoveryFabricPlanV1 *plan)
{
	UT_ASSERT(plan == (ClusterThreadRecoveryFabricPlanV1 *)&fabric_object);
	plan_seal_count++;
	if (expire_during_seal)
		stale_origin = 2;
	return RF_PAGE_PROOF_DETAIL_OK;
}

bool
cluster_thread_recovery_fabric_bind_database_v1(ClusterThreadRecoveryFabricPlanV1 *plan,
	uint64 database_incarnation)
{
	UT_ASSERT(plan == (ClusterThreadRecoveryFabricPlanV1 *)&fabric_object);
	UT_ASSERT_EQ(plan_feed_count, 2 * expected_participants);
	UT_ASSERT_EQ(plan_seal_count, 0);
	database_bind_count++;
	return database_incarnation == 42;
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
	static ClusterControlRootReadToken token;

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
	authority->root_token = &token;
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
	cluster_shared_config = false;
	exact_source_count = exact_source_fault = 0;
	database_bind_count = 0;
	expected_participants = 1;
	fault_origin = stale_origin = 0;
	expire_during_seal = false;
}

static void
init_two(ClusterThreadRecoveryAuthorityV1 authorities[2], ClusterRecoveryDutyKey duties[2],
	ClusterControlRootSnapshot roots[2], ClusterControlRootReadToken tokens[2])
{
	init_case(&authorities[0]);
	duties[0] = *authorities[0].duty;
	roots[0] = *authorities[0].root_snapshot;
	tokens[0] = *authorities[0].root_token;
	authorities[1] = authorities[0];
	duties[1] = duties[0];
	duties[1].origin_thread_id = 4;
	roots[1] = roots[0];
	roots[1].identity = duties[1];
	roots[1].checkpoint_tli = roots[1].tail_tli = 8;
	tokens[1] = tokens[0];
	for (int i = 0; i < 2; i++) {
		authorities[i].duty = &duties[i];
		authorities[i].root_snapshot = &roots[i];
		authorities[i].root_token = &tokens[i];
	}
	expected_participants = 2;
	cluster_shared_config = true;
}

UT_TEST(test_multi_source_seals_one_plan_after_every_original_cut)
{
	ClusterThreadRecoveryAuthorityV1 authorities[2];
	ClusterRecoveryDutyKey duties[2];
	ClusterControlRootSnapshot roots[2];
	ClusterControlRootReadToken tokens[2];
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	uint64 records = 99;

	init_two(authorities, duties, roots, tokens);
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_scan_roots_v1(authorities, 2, false, &plan,
		&records), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(plan != NULL && records == 4);
	UT_ASSERT_EQ(plan_create_count, 1);
	UT_ASSERT_EQ(plan_seal_count, 1);
	UT_ASSERT_EQ(plan_feed_count, 4);
	UT_ASSERT_EQ(exact_source_count, 2);
	UT_ASSERT_EQ(database_bind_count, 1);
	UT_ASSERT_EQ(reader_make_count, 0);
}

UT_TEST(test_multi_source_late_refusal_discards_earlier_source_too)
{
	for (int fault = 1; fault <= 6; fault++) {
		ClusterThreadRecoveryAuthorityV1 authorities[2];
		ClusterRecoveryDutyKey duties[2];
		ClusterControlRootSnapshot roots[2];
		ClusterControlRootReadToken tokens[2];
		ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
		uint64 records = 99;
		volatile bool caught = false;

		init_two(authorities, duties, roots, tokens);
		fault_origin = 4;
		exact_source_fault = fault;
		PG_TRY();
		{
			UT_ASSERT(cluster_thread_recovery_fabric_scan_roots_v1(authorities, 2, false,
				&plan, &records) != RF_PAGE_PROOF_DETAIL_OK);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 3);
		UT_ASSERT(plan == NULL && records == 0);
		UT_ASSERT_EQ(exact_source_count, 2);
		UT_ASSERT_EQ(plan_destroy_count, 1);
		UT_ASSERT_EQ(plan_seal_count, 0);
	}
}

UT_TEST(test_multi_source_wrong_cut_namespace_or_pin_refuses_before_scan)
{
	for (int fault = 1; fault <= 5; fault++) {
		ClusterThreadRecoveryAuthorityV1 authorities[2];
		ClusterRecoveryDutyKey duties[2];
		ClusterControlRootSnapshot roots[2];
		ClusterControlRootReadToken tokens[2];
		ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
		uint64 records = 99;

		init_two(authorities, duties, roots, tokens);
		if (fault == 1) duties[1].system_identifier++;
		if (fault == 2) duties[1].storage_uuid[0]++;
		if (fault == 3) authorities[1].retention_pin = (ClusterWalRetentionPin *)&fabric_object;
		if (fault == 4) roots[1].tail_tli++;
		if (fault == 5) { duties[1].origin_thread_id = 2; roots[1].identity = duties[1]; }
		UT_ASSERT(cluster_thread_recovery_fabric_scan_roots_v1(authorities, 2, false,
			&plan, &records) != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(plan == NULL && records == 0);
		UT_ASSERT_EQ(exact_source_count, 0);
		UT_ASSERT_EQ(plan_create_count, 0);
	}
}

UT_TEST(test_multi_source_rechecks_every_owner_after_seal)
{
	ClusterThreadRecoveryAuthorityV1 authorities[2];
	ClusterRecoveryDutyKey duties[2];
	ClusterControlRootSnapshot roots[2];
	ClusterControlRootReadToken tokens[2];
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	uint64 records = 99;

	init_two(authorities, duties, roots, tokens);
	expire_during_seal = true;
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_scan_roots_v1(authorities, 2, false,
		&plan, &records), RF_PAGE_PROOF_DETAIL_ROOT_STALE);
	UT_ASSERT(plan == NULL && records == 0);
	UT_ASSERT_EQ(plan_seal_count, 1);
	UT_ASSERT_EQ(plan_destroy_count, 1);
}

UT_TEST(test_shared_config_uses_exact_source_without_legacy_fallback)
{
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	uint64 records = 0;

	init_case(&authority);
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_scan_root_v1(2, 0x100, 0x200, &authority, false,
															 &plan, &records),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(plan == (ClusterThreadRecoveryFabricPlanV1 *)&fabric_object && records == 2);
	UT_ASSERT_EQ(reader_make_count, 0);
	UT_ASSERT_EQ(exact_source_count, 1);
	UT_ASSERT_EQ(database_bind_count, 1);
	UT_ASSERT_EQ(plan_feed_count, 2);
	UT_ASSERT_EQ(plan_seal_count, 1);
	UT_ASSERT_EQ(plan_destroy_count, 0);
}

UT_TEST(test_shared_source_failure_discards_every_provisional_record)
{
	for (int fault = 1; fault <= 4; fault++) {
		ClusterThreadRecoveryAuthorityV1 authority;
		ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
		uint64 records = 7;
		volatile bool caught = false;
		init_case(&authority);
		cluster_shared_config = true;
		exact_source_fault = fault;
		PG_TRY();
		{
			RfPageProofDetailV1 detail = cluster_thread_recovery_fabric_scan_root_v1(
				2, 0x100, 0x200, &authority, false, &plan, &records);
			UT_ASSERT_EQ(detail, fault == 1 ? RF_PAGE_PROOF_DETAIL_ROOT_STALE
				: fault == 4 ? RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH : RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 3);
		UT_ASSERT(plan == NULL && records == 0);
		UT_ASSERT_EQ(plan_seal_count, 0);
		UT_ASSERT_EQ(plan_destroy_count, 1);
		UT_ASSERT_EQ(reader_make_count, 0);
	}
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
	UT_PLAN(10);
	UT_RUN(test_shared_config_uses_exact_source_without_legacy_fallback);
	UT_RUN(test_shared_source_failure_discards_every_provisional_record);
	UT_RUN(test_scans_exact_root_cut_and_seals_only_at_upper_boundary);
	UT_RUN(test_early_end_destroys_unsealed_plan);
	UT_RUN(test_feed_failure_poisons_and_destroys_whole_plan);
	UT_RUN(test_non_root_window_is_rejected_before_reader_or_plan);
	UT_RUN(test_multi_source_seals_one_plan_after_every_original_cut);
	UT_RUN(test_multi_source_late_refusal_discards_earlier_source_too);
	UT_RUN(test_multi_source_wrong_cut_namespace_or_pin_refuses_before_scan);
	UT_RUN(test_multi_source_rechecks_every_owner_after_seal);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
