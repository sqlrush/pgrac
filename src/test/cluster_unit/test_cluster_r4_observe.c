/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_observe.c
 *	Stage 8 R4 D11 observation adapter and holder linearization points.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_r4_observe.h"
#include "cluster/cluster_semantic_activation.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
int cluster_node_id = 1;
static int refusal_log_calls;

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	UT_ASSERT_EQ(elevel, LOG);
	refusal_log_calls++;
	return true;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{}

static uint64 observed[CLUSTER_R4_OBSERVATION_EVENT_COUNT];
static ClusterSemanticAdmissionResult admission_result;
static bool admission_recheck;
static bool copy_ok;
static ClusterBufmgrGcsCopyRefusal copy_refusal;
static bool construct_partial;
static int enter_calls;
static int recheck_calls;
static int leave_calls;

void
cluster_cr_r4_event_bump(uint32 event)
{
	UT_ASSERT(event < CLUSTER_R4_OBSERVATION_EVENT_COUNT);
	if (event < CLUSTER_R4_OBSERVATION_EVENT_COUNT)
		observed[event]++;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	enter_calls++;
	UT_ASSERT_EQ(feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(side, CLUSTER_SEMANTIC_TARGET_SIDE);
	memset(token, 0, sizeof(*token));
	if (admission_result == CLUSTER_SEMANTIC_ADMISSION_OK) {
		token->feature_bit = feature_bit;
		token->side = (uint8)side;
		token->entered = true;
	}
	return admission_result;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	recheck_calls++;
	UT_ASSERT(token != NULL && token->entered);
	return admission_recheck;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	leave_calls++;
	UT_ASSERT(token != NULL && token->entered);
	token->entered = false;
}

bool
cluster_bufmgr_copy_block_for_r4_cr(BufferTag tag pg_attribute_unused(),
									SCN expected_page_scn pg_attribute_unused(),
									XLogRecPtr *page_lsn_out, SCN *page_scn_out,
									char *dst, ClusterBufmgrGcsCopyRefusal *refusal_out)
{
	if (page_lsn_out != NULL)
		*page_lsn_out = (XLogRecPtr)1;
	if (page_scn_out != NULL)
		*page_scn_out = (SCN)2;
	if (dst != NULL)
		memset(dst, 0x5a, BLCKSZ);
	if (refusal_out != NULL)
		*refusal_out = copy_refusal;
	return copy_ok;
}

void
cluster_cr_construct_page_for_server(const char *cur_page pg_attribute_unused(),
									 SCN read_scn pg_attribute_unused(),
									 BufferTag tag pg_attribute_unused(), char *dst_page,
									 bool *out_partial)
{
	memset(dst_page, 0x6b, BLCKSZ);
	*out_partial = construct_partial;
}

void
FlushErrorState(void)
{
}

void
pg_re_throw(void)
{
	abort();
}

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

static void
reset_fixture(void)
{
	memset(observed, 0, sizeof(observed));
	admission_result = CLUSTER_SEMANTIC_ADMISSION_OK;
	admission_recheck = true;
	copy_ok = true;
	copy_refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
	construct_partial = false;
	enter_calls = 0;
	recheck_calls = 0;
	leave_calls = 0;
}

static uint64
observation_total(void)
{
	uint64 total = 0;
	int i;

	for (i = 0; i < CLUSTER_R4_OBSERVATION_EVENT_COUNT; i++)
		total += observed[i];
	return total;
}

UT_TEST(test_event_domain_is_exact_and_adapter_is_one_to_one)
{
	int i;

	reset_fixture();
	UT_ASSERT_EQ(CLUSTER_R4_EVENT_CR_ROUTE_STARTED, 0);
	UT_ASSERT_EQ(CLUSTER_R4_EVENT_CR_HOLDER_FULL, 1);
	UT_ASSERT_EQ(CLUSTER_R4_EVENT_TX_UNKNOWN, 6);
	UT_ASSERT_EQ(CLUSTER_R4_EVENT_TX_ABORTED, 10);
	UT_ASSERT_EQ(CLUSTER_R4_EVENT_SLOT_CAPACITY_RETRY, 13);
	UT_ASSERT_EQ(CLUSTER_R4_EVENT_COUNT, 9);
	UT_ASSERT_EQ(CLUSTER_R4_OBSERVATION_EVENT_COUNT, 71);
	for (i = 0; i < CLUSTER_R4_OBSERVATION_EVENT_COUNT; i++)
		cluster_r4_observe((ClusterR4Event)i, CLUSTER_TX_RESOLVE_NONE,
						   CLUSTER_CR_BUILD_NONE);
	for (i = 0; i < CLUSTER_R4_OBSERVATION_EVENT_COUNT; i++)
		UT_ASSERT_EQ(observed[i], 1);
	cluster_r4_observe((ClusterR4Event)-1, CLUSTER_TX_RESOLVE_PROTOCOL,
					   CLUSTER_CR_BUILD_PROTOCOL);
	cluster_r4_observe((ClusterR4Event)CLUSTER_R4_OBSERVATION_EVENT_COUNT,
					   CLUSTER_TX_RESOLVE_PROTOCOL, CLUSTER_CR_BUILD_PROTOCOL);
	UT_ASSERT_EQ(observation_total(), CLUSTER_R4_OBSERVATION_EVENT_COUNT);
}

UT_TEST(test_holder_full_is_observed_after_accepted_build)
{
	BufferTag tag = {0};
	char page[BLCKSZ];
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;

	reset_fixture();
	UT_ASSERT_EQ(cluster_cr_build_on_holder(&tag, (SCN)1, page, &reason),
				 CLUSTER_CR_BUILD_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(observed[CLUSTER_R4_EVENT_CR_HOLDER_FULL], 1);
	UT_ASSERT_EQ(observation_total(), 1);
	UT_ASSERT_EQ(enter_calls, 1);
	UT_ASSERT_EQ(recheck_calls, 1);
	UT_ASSERT_EQ(leave_calls, 1);
}

UT_TEST(test_holder_retry_is_observed_with_typed_reason)
{
	BufferTag tag = {0};
	char page[BLCKSZ];
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;

	reset_fixture();
	copy_ok = false;
	copy_refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NOT_RESIDENT;
	UT_ASSERT_EQ(cluster_cr_build_on_holder(&tag, (SCN)1, page, &reason),
				 CLUSTER_CR_BUILD_RETRYABLE);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_HOLDER_MOVED);
	UT_ASSERT_EQ(observed[CLUSTER_R4_EVENT_CR_HOLDER_RETRY], 1);
	UT_ASSERT_EQ(observation_total(), 1);
}

UT_TEST(test_holder_nonretryable_failure_is_observed)
{
	char page[BLCKSZ];
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;

	reset_fixture();
	UT_ASSERT_EQ(cluster_cr_build_on_holder(NULL, (SCN)1, page, &reason),
				 CLUSTER_CR_BUILD_FAIL_CLOSED);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);
	UT_ASSERT_EQ(observed[CLUSTER_R4_EVENT_CR_HOLDER_FAIL_CLOSED], 1);
	UT_ASSERT_EQ(observation_total(), 1);
}

UT_TEST(test_holder_admission_refusal_is_not_an_event)
{
	BufferTag tag = {0};
	char page[BLCKSZ];
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;

	reset_fixture();
	admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	UT_ASSERT_EQ(cluster_cr_build_on_holder(&tag, (SCN)1, page, &reason),
				 CLUSTER_CR_BUILD_RETRYABLE);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_TARGET_DISABLED);
	UT_ASSERT_EQ(observation_total(), 0);
	UT_ASSERT_EQ(enter_calls, 1);
	UT_ASSERT_EQ(recheck_calls, 0);
	UT_ASSERT_EQ(leave_calls, 0);
}

UT_TEST(test_refusal_reasons_are_stage_separated_and_logs_are_bounded)
{
	BufferTag tag = { 0 };
	int stage;
	int reason;
	int repetition;

	reset_fixture();
	refusal_log_calls = 0;
	for (stage = 0; stage < CLUSTER_R4_REFUSAL_STAGE_COUNT; stage++)
		for (reason = CLUSTER_CR_BUILD_TARGET_DISABLED; reason <= CLUSTER_CR_BUILD_PROTOCOL;
			 reason++) {
			for (repetition = 0; repetition < 4; repetition++)
				cluster_r4_observe_refusal((ClusterR4RefusalStage)stage,
										   (ClusterCrBuildReason)reason, &tag, 42, 9, 2, 1,
										   (SCN)88);
			UT_ASSERT_EQ(observed[CLUSTER_R4_REFUSAL_EVENT(stage, reason)], 4);
		}
	UT_ASSERT_EQ(observation_total(), 3 * 17 * 4);
	UT_ASSERT_EQ(refusal_log_calls, 3 * 17 * 3); /* Calls 1, 2 and 4, not 3. */
	cluster_r4_observe_refusal(CLUSTER_R4_REFUSAL_MASTER, CLUSTER_CR_BUILD_NONE, &tag, 42, 9, 2, 1,
							   (SCN)88);
	cluster_r4_observe_refusal((ClusterR4RefusalStage)-1, CLUSTER_CR_BUILD_PROTOCOL, &tag, 42, 9, 2,
							   1, (SCN)88);
	cluster_r4_observe_refusal(CLUSTER_R4_REFUSAL_HOLDER_SHIP, CLUSTER_CR_BUILD_PROTOCOL, NULL, 42,
							   9, 2, 1, (SCN)88);
	UT_ASSERT_EQ(observation_total(), 3 * 17 * 4);
	UT_ASSERT_EQ(refusal_log_calls, 3 * 17 * 3);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_event_domain_is_exact_and_adapter_is_one_to_one);
	UT_RUN(test_holder_full_is_observed_after_accepted_build);
	UT_RUN(test_holder_retry_is_observed_with_typed_reason);
	UT_RUN(test_holder_nonretryable_failure_is_observed);
	UT_RUN(test_holder_admission_refusal_is_not_an_event);
	UT_RUN(test_refusal_reasons_are_stage_separated_and_logs_are_bounded);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
