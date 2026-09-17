/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_d10_hint_source.c
 *	  R4 D10 dormant-source admission tests for TT status hints.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_r4_d10_hint_source.c
 *
 * NOTES
 *	  This is a pgrac-original file.  It executes the product dispatch
 *	  against real hint queue state while replacing only adjacent process,
 *	  transport and shared-admission boundaries.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_status_hint.h"
#include "cluster/cluster_tx_enqueue.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define TEST_HINT_RING_BYTES (64 * 1024)

typedef union TestHintStorage {
	uint64 align;
	uint8 bytes[TEST_HINT_RING_BYTES];
} TestHintStorage;

static TestHintStorage test_hint_ring;
static TestHintStorage test_hint_counters;
static TestHintStorage test_multi_ring;
static ClusterConf test_cluster_conf;
static ClusterSemanticAdmissionResult test_admission_result;
static bool test_recheck_result;
static int test_leave_count;
static const ClusterICMsgTypeInfo *test_registered_msg_info;

bool cluster_enabled = true;
int cluster_node_id = 0;
int cluster_tt_status_hint_outbound_capacity = 4;
int cluster_tt_status_hint_emit_mode = CLUSTER_TT_STATUS_HINT_EMIT_ALL_STATUS;
int cluster_multixact_hint_outbound_slots = 4;
int cluster_multixact_member_overlay_max_members = 4;
ClusterConf *ClusterConfShmem = &test_cluster_conf;
ProcessingMode Mode = NormalProcessing;
BackendType MyBackendType = B_BACKEND;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
cluster_vis_evidence_note(ClusterVisEvidenceMetric metric pg_attribute_unused())
{
	/* Diagnostics are not the source-admission policy under test. */
}

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	for (int i = 0; i < test_cluster_conf.node_count; i++)
		if (test_cluster_conf.nodes[i].node_id == node_id)
			return &test_cluster_conf.nodes[i];
	return NULL;
}

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

void *
ShmemInitStruct(const char *name, Size size, bool *found_ptr)
{
	TestHintStorage *storage;

	if (strcmp(name, "ClusterTTStatusHintOutbound") == 0)
		storage = &test_hint_ring;
	else if (strcmp(name, "ClusterTTStatusHintState") == 0)
		storage = &test_hint_counters;
	else if (strcmp(name, "ClusterMultiXactHintOutbound") == 0)
		storage = &test_multi_ring;
	else
		abort();
	if (size > sizeof(storage->bytes))
		abort();
	memset(storage->bytes, 0, sizeof(storage->bytes));
	*found_ptr = false;
	return storage->bytes;
}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

void
cluster_lmon_duty_mark_dirty(ClusterLmonDuty duty pg_attribute_unused())
{}

void
cluster_lmon_wakeup(void)
{}

void
cluster_ic_send_envelope_fanout(uint8 msg_type pg_attribute_unused(),
								const void *payload pg_attribute_unused(),
								uint32 payload_len pg_attribute_unused(),
								ClusterICFanoutResult per_peer[] pg_attribute_unused())
{}

void
cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *info)
{
	test_registered_msg_info = info;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

uint64
cluster_epoch_get_current(void)
{
	return 1;
}

ClusterSemanticAdmissionResult
cluster_multixact_source_dispatch(ClusterMultiXactSourceOp op pg_attribute_unused(),
								  const ClusterMultiXactSourceRequest *request
									  pg_attribute_unused(),
								  ClusterMultiXactSourceResult *result)
{
	memset(result, 0, sizeof(*result));
	result->bool_value = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp op pg_attribute_unused(),
								  const ClusterTTStatusSourceRequest *request pg_attribute_unused(),
								  ClusterTTStatusSourceResult *result)
{
	memset(result, 0, sizeof(*result));
	result->bool_value = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

void
cluster_txw_wake_waiters(const ClusterTTStatusKey *holder_key pg_attribute_unused())
{}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

ErrorData *
CopyErrorData(void)
{
	static ErrorData error_data;

	return &error_data;
}

void
FlushErrorState(void)
{}

void
ReThrowError(ErrorData *edata pg_attribute_unused())
{
	abort();
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit pg_attribute_unused(),
								  ClusterSemanticAdmissionSide side pg_attribute_unused(),
								  ClusterSemanticAdmissionToken *token)
{
	memset(token, 0, sizeof(*token));
	if (test_admission_result == CLUSTER_SEMANTIC_ADMISSION_OK) {
		token->feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		token->side = CLUSTER_SEMANTIC_SOURCE_SIDE;
		token->entered = true;
	}
	return test_admission_result;
}

bool
cluster_semantic_activation_recheck(
	const ClusterSemanticAdmissionToken *token pg_attribute_unused())
{
	return test_recheck_result;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	test_leave_count++;
	memset(token, 0, sizeof(*token));
}

static void
test_reset_gate(ClusterSemanticAdmissionResult admission_result, bool recheck_result)
{
	test_admission_result = admission_result;
	test_recheck_result = recheck_result;
	test_leave_count = 0;
}

static void
test_prepare_hint_state(void)
{
	memset(&test_cluster_conf, 0, sizeof(test_cluster_conf));
	test_cluster_conf.node_count = 2;
	test_cluster_conf.nodes[0].node_id = 0;
	test_cluster_conf.nodes[1].node_id = 1;
	cluster_tt_status_hint_shmem_init();
}

UT_TEST(test_active_refuses_all_ops_before_request_inspection)
{
	const ClusterTTStatusHintSourceRequest *poison_request
		= (const ClusterTTStatusHintSourceRequest *)(uintptr_t)1;
	ClusterTTStatusHintSourceOp op;

	test_reset_gate(CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT, true);
	for (op = CLUSTER_TT_HINT_SOURCE_EMIT; op <= CLUSTER_TT_HINT_SOURCE_DRAIN_OUTBOUND; op++)
		UT_ASSERT_EQ(cluster_tt_status_hint_source_dispatch(op, poison_request),
					 CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT);
	UT_ASSERT_EQ(test_leave_count, 0);
	UT_ASSERT_EQ(cluster_tt_status_hint_get_emit_count(), 0);
}

UT_TEST(test_disabled_emit_mutates_real_source_queue)
{
	ClusterTTStatusKey key;
	ClusterTTStatusHintSourceRequest request;

	memset(&key, 0, sizeof(key));
	memset(&request, 0, sizeof(request));
	request.key = &key;
	request.status = CLUSTER_TT_STATUS_COMMITTED;
	request.commit_scn = 42;
	test_reset_gate(CLUSTER_SEMANTIC_ADMISSION_OK, true);

	UT_ASSERT_EQ(cluster_tt_status_hint_source_dispatch(CLUSTER_TT_HINT_SOURCE_EMIT, &request),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(cluster_tt_status_hint_get_emit_count(), 1);
	UT_ASSERT_EQ(test_leave_count, 1);
}

UT_TEST(test_validation_happens_after_admission_and_leaves_once)
{
	ClusterTTStatusHintSourceRequest request;

	memset(&request, 0, sizeof(request));
	test_reset_gate(CLUSTER_SEMANTIC_ADMISSION_OK, true);

	UT_ASSERT_EQ(cluster_tt_status_hint_source_dispatch(CLUSTER_TT_HINT_SOURCE_EMIT, &request),
				 CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT_EQ(test_leave_count, 1);
	UT_ASSERT_EQ(cluster_tt_status_hint_get_emit_count(), 1);
}

UT_TEST(test_recheck_drift_returns_generation_changed_and_leaves)
{
	ClusterTTStatusKey key;
	ClusterTTStatusHintSourceRequest request;

	memset(&key, 0, sizeof(key));
	memset(&request, 0, sizeof(request));
	request.key = &key;
	request.status = CLUSTER_TT_STATUS_ABORTED;
	request.commit_scn = InvalidScn;
	test_reset_gate(CLUSTER_SEMANTIC_ADMISSION_OK, false);

	UT_ASSERT_EQ(cluster_tt_status_hint_source_dispatch(CLUSTER_TT_HINT_SOURCE_EMIT, &request),
				 CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED);
	UT_ASSERT_EQ(cluster_tt_status_hint_get_emit_count(), 2);
	UT_ASSERT_EQ(test_leave_count, 1);
}

UT_TEST(test_registration_adapter_uses_source_dispatch)
{
	const ClusterICEnvelope *poison_env = (const ClusterICEnvelope *)(uintptr_t)1;
	const void *poison_payload = (const void *)(uintptr_t)1;

	test_registered_msg_info = NULL;
	cluster_tt_status_hint_register_msg_type();
	UT_ASSERT_NOT_NULL(test_registered_msg_info);
	UT_ASSERT_NOT_NULL(test_registered_msg_info->handler);
	test_reset_gate(CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT, true);
	test_registered_msg_info->handler(poison_env, poison_payload);
	UT_ASSERT_EQ(test_leave_count, 0);
	UT_ASSERT_EQ(cluster_tt_status_hint_get_receive_count(), 0);
}

int
main(void)
{
	test_prepare_hint_state();
	UT_PLAN(5);
	UT_RUN(test_active_refuses_all_ops_before_request_inspection);
	UT_RUN(test_disabled_emit_mutates_real_source_queue);
	UT_RUN(test_validation_happens_after_admission_and_leaves_once);
	UT_RUN(test_recheck_drift_returns_generation_changed_and_leaves);
	UT_RUN(test_registration_adapter_uses_source_dispatch);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
