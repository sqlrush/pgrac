/*-------------------------------------------------------------------------
 *
 * test_cluster_service_observe.c
 *    Actual native-service census with isolated original-module boundaries.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_service_observe.c
 *
 * NOTES
 *    Module return values are fixtures, not module retirement or a global
 *    certificate. The fanout, result priority and role selection are real C.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_service_observe.h"
#include "cluster/cluster_control_request.h"
#include "cluster/cluster_guc.h"
#include "miscadmin.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool IsUnderPostmaster = true;
BackendType MyBackendType;
AuxProcType MyAuxProcType;
bool cluster_shared_config = true;

enum {
	GRD_WORK,
	GRD_OUT,
	CR,
	PROBE,
	GCS,
	IC,
	SEMANTIC,
	SCN_OWNER,
	RECONFIG,
	CLOSE,
	REMOVE,
	FENCE,
	WRITE_FENCE,
	CF,
	RECOVERY,
	BACKUP,
	MRP,
	GCS_DEDUP,
	GES_DEDUP,
	LMD_PROBE,
	CONTROL_REQUEST,
	LMS_OUT,
	LMD,
	LMD_PENDING,
	LMD_GRAPH,
	SINVAL,
	KO,
	MODULES
};
static ClusterNormalStopPollResult answers[MODULES];
static uint64 seen;
static unsigned calls;
static unsigned stop_close_calls, online_close_calls;

static ClusterNormalStopPollResult
answer(unsigned module, const char **reason)
{
	UT_ASSERT(module < MODULES && reason != NULL);
	UT_ASSERT((seen & (UINT64CONST(1) << module)) == 0);
	seen |= UINT64CONST(1) << module;
	++calls;
	*reason = "fixture original owner";
	return answers[module];
}

#define SIMPLE_POLL(name, index, type)                                                             \
	ClusterNormalStopPollResult name(type *key, const char **reason)                               \
	{                                                                                              \
		*key = (type)(100 + index);                                                                \
		return answer(index, reason);                                                              \
	}
#define DOMAIN_POLL(name, index)                                                                   \
	ClusterNormalStopPollResult name(const char **domain, uint64 *key, const char **reason)        \
	{                                                                                              \
		*domain = #index;                                                                          \
		*key = 100 + index;                                                                        \
		return answer(index, reason);                                                              \
	}
SIMPLE_POLL(cluster_grd_work_queue_normal_stop_poll, GRD_WORK, uint32)
SIMPLE_POLL(cluster_grd_outbound_normal_stop_poll, GRD_OUT, uint32)
SIMPLE_POLL(cluster_cr_server_normal_stop_poll, CR, int)
SIMPLE_POLL(cluster_lms_native_probe_normal_stop_poll, PROBE, int)
SIMPLE_POLL(cluster_gcs_block_normal_stop_local_poll, GCS, int)
SIMPLE_POLL(cluster_lmd_probe_normal_stop_poll, LMD_PROBE, uint64)
SIMPLE_POLL(cluster_lmd_pending_normal_stop_poll, LMD_PENDING, uint64)
SIMPLE_POLL(cluster_lmd_graph_normal_stop_poll, LMD_GRAPH, uint64)
SIMPLE_POLL(cluster_ko_normal_stop_poll, KO, uint32)
DOMAIN_POLL(cluster_semantic_normal_stop_poll, SEMANTIC)
DOMAIN_POLL(cluster_scn_normal_stop_poll, SCN_OWNER)
DOMAIN_POLL(cluster_reconfig_normal_stop_poll, RECONFIG)
DOMAIN_POLL(cluster_node_remove_normal_stop_poll, REMOVE)
DOMAIN_POLL(cluster_fence_normal_stop_poll, FENCE)
DOMAIN_POLL(cluster_write_fence_normal_stop_poll, WRITE_FENCE)
DOMAIN_POLL(cluster_recovery_normal_stop_poll, RECOVERY)
DOMAIN_POLL(cluster_backup_normal_stop_poll, BACKUP)
DOMAIN_POLL(cluster_mrp_normal_stop_poll, MRP)
DOMAIN_POLL(cluster_gcs_dedup_normal_stop_poll, GCS_DEDUP)
DOMAIN_POLL(cluster_ges_dedup_normal_stop_poll, GES_DEDUP)
DOMAIN_POLL(cluster_lmd_normal_stop_poll, LMD)
DOMAIN_POLL(cluster_sinval_normal_stop_poll, SINVAL)

ClusterNormalStopPollResult
cluster_clean_leave_normal_stop_local_poll(int *peer, const char **reason)
{
	++stop_close_calls;
	*peer = 100 + CLOSE;
	return answer(CLOSE, reason);
}

ClusterNormalStopPollResult
cluster_clean_leave_service_poll(int *peer, const char **reason)
{
	++online_close_calls;
	*peer = 100 + CLOSE;
	return answer(CLOSE, reason);
}

ClusterNormalStopPollResult
cluster_ic_normal_stop_poll(const char **domain, int *peer, uint32 *sequence, const char **reason)
{
	*domain = "IC";
	*peer = 4;
	*sequence = 91;
	return answer(IC, reason);
}
ClusterNormalStopPollResult
cluster_lms_outbound_normal_stop_poll(int *worker, uint32 *position, const char **reason)
{
	*worker = 3;
	*position = 92;
	return answer(LMS_OUT, reason);
}
ClusterNormalStopPollResult
cluster_cf_normal_stop_poll(bool post_checkpoint, const char **reason)
{
	UT_ASSERT(!post_checkpoint);
	return answer(CF, reason);
}
bool
cluster_control_request_empty(void)
{
	const char *reason;
	return answer(CONTROL_REQUEST, &reason) == CLUSTER_NORMAL_STOP_READY;
}

static void
reset(BackendType role, AuxProcType aux)
{
	IsUnderPostmaster = true;
	cluster_shared_config = true;
	MyBackendType = role;
	MyAuxProcType = aux;
	seen = calls = 0;
	stop_close_calls = online_close_calls = 0;
	for (unsigned i = 0; i < MODULES; ++i)
		answers[i] = CLUSTER_NORMAL_STOP_READY;
}

typedef struct FixtureRole {
	BackendType role;
	AuxProcType aux;
	uint64 mask;
	unsigned count;
} FixtureRole;
#define BIT(m) (UINT64CONST(1) << (m))
static const FixtureRole roles[]
	= { { B_LMON, LmonProcess, (BIT(CONTROL_REQUEST + 1) - 1), 21 },
		{ B_LMS, LmsProcess, BIT(CR) | BIT(PROBE) | BIT(GCS) | BIT(LMS_OUT) | BIT(IC), 5 },
		{ B_LMS_WORKER, LmsWorker1Process, BIT(CR) | BIT(PROBE) | BIT(GCS) | BIT(LMS_OUT) | BIT(IC),
		  5 },
		{ B_LMS_WORKER, LmsWorker7Process, BIT(CR) | BIT(PROBE) | BIT(GCS) | BIT(LMS_OUT) | BIT(IC),
		  5 },
		{ B_LMD, LmdProcess, BIT(LMD) | BIT(LMD_PENDING) | BIT(LMD_GRAPH) | BIT(LMD_PROBE), 4 },
		{ B_SINVAL_BCAST, SinvalBcastProcess, BIT(SINVAL) | BIT(KO), 2 } };

UT_TEST(native_fanouts_are_exact_without_shutdown)
{
	for (unsigned r = 0; r < lengthof(roles); ++r) {
		ClusterServiceObservation out;
		reset(roles[r].role, roles[r].aux);
		UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(seen, roles[r].mask);
		UT_ASSERT_EQ(calls, roles[r].count);
		UT_ASSERT_EQ(out.modules, calls);
		UT_ASSERT(strcmp(out.reason, "NONE") == 0);
		UT_ASSERT_EQ(out.slot, -1);
		UT_ASSERT_EQ(stop_close_calls, 0);
		UT_ASSERT_EQ(online_close_calls, roles[r].role == B_LMON ? 1 : 0);
	}
}
UT_TEST(shutdown_keeps_original_close_stage_instead_of_online_busy_rule)
{
	for (unsigned r = 0; r < lengthof(roles); ++r) {
		ClusterServiceObservation out;
		reset(roles[r].role, roles[r].aux);
		UT_ASSERT_EQ(cluster_service_normal_stop_observe(&out), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(seen, roles[r].mask);
		UT_ASSERT_EQ(calls, roles[r].count);
		UT_ASSERT_EQ(online_close_calls, 0);
		UT_ASSERT_EQ(stop_close_calls, roles[r].role == B_LMON ? 1 : 0);
	}
}
UT_TEST(every_original_pending_owner_prevents_ready)
{
	for (unsigned r = 0; r < lengthof(roles); ++r)
		for (unsigned m = 0; m < MODULES; ++m) {
			ClusterServiceObservation out;
			if (!(roles[r].mask & BIT(m)))
				continue;
			reset(roles[r].role, roles[r].aux);
			answers[m] = CLUSTER_NORMAL_STOP_PENDING;
			UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_PENDING);
			UT_ASSERT_EQ(seen, roles[r].mask);
			UT_ASSERT_EQ(calls, out.modules);
			UT_ASSERT(strcmp(out.reason, "NONE") != 0);
			UT_ASSERT_EQ(answers[m], CLUSTER_NORMAL_STOP_PENDING);
		}
}
UT_TEST(every_invalid_owner_is_not_a_pending_or_empty_owner)
{
	for (unsigned r = 0; r < lengthof(roles); ++r)
		for (unsigned m = 0; m < MODULES; ++m) {
			ClusterServiceObservation out;
			if (!(roles[r].mask & BIT(m)) || m == CONTROL_REQUEST)
				continue; /* The actual control empty predicate returns bool. */
			reset(roles[r].role, roles[r].aux);
			answers[m] = CLUSTER_NORMAL_STOP_INVALID;
			UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
			UT_ASSERT_EQ(seen, roles[r].mask);
			UT_ASSERT_EQ(calls, out.modules);
		}
}
UT_TEST(later_invalid_dominates_without_short_circuit)
{
	ClusterServiceObservation out;
	reset(B_LMS, LmsProcess);
	answers[CR] = CLUSTER_NORMAL_STOP_PENDING;
	answers[GCS] = CLUSTER_NORMAL_STOP_INVALID;
	UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(strcmp(out.domain, "GCS_LOCAL") == 0);
	UT_ASSERT_EQ(out.slot, 100 + GCS);
	UT_ASSERT_EQ(seen, roles[1].mask);
	answers[PROBE] = (ClusterNormalStopPollResult)91;
	seen = calls = 0;
	UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(strcmp(out.domain, "NATIVE_PROBE") == 0);
	UT_ASSERT_EQ(seen, roles[1].mask);
}
UT_TEST(wrong_or_missing_native_owner_is_not_observed)
{
	ClusterServiceObservation out;
	for (unsigned r = 0; r < lengthof(roles); ++r) {
		reset(roles[r].role, roles[r].aux);
		IsUnderPostmaster = false;
		UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(calls, 0);
		IsUnderPostmaster = true;
		MyBackendType = B_BACKEND;
		UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(calls, 0);
		MyBackendType = roles[r].role;
		MyAuxProcType = NotAnAuxProcess;
		UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(calls, 0);
	}
	UT_ASSERT_EQ(cluster_service_observe(NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(calls, 0);
}
UT_TEST(control_request_is_not_excluded_for_own_configuration)
{
	ClusterServiceObservation out;
	reset(B_LMON, LmonProcess);
	answers[CONTROL_REQUEST] = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(strcmp(out.domain, "CONTROL_REQUEST") == 0);
	seen = calls = 0;
	answers[CONTROL_REQUEST] = CLUSTER_NORMAL_STOP_READY;
	UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_READY);
	seen = calls = 0;
	cluster_shared_config = false;
	UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(seen, roles[0].mask & ~BIT(CONTROL_REQUEST));
}
UT_TEST(original_transport_position_is_preserved)
{
	ClusterServiceObservation out;
	reset(B_LMS_WORKER, LmsWorker4Process);
	answers[IC] = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(out.position, 91);
	UT_ASSERT_EQ(out.slot, 4);
	seen = calls = 0;
	answers[LMS_OUT] = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(out.position, 92);
	UT_ASSERT_EQ(out.slot, 3);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(native_fanouts_are_exact_without_shutdown);
	UT_RUN(shutdown_keeps_original_close_stage_instead_of_online_busy_rule);
	UT_RUN(every_original_pending_owner_prevents_ready);
	UT_RUN(every_invalid_owner_is_not_a_pending_or_empty_owner);
	UT_RUN(later_invalid_dominates_without_short_circuit);
	UT_RUN(wrong_or_missing_native_owner_is_not_observed);
	UT_RUN(control_request_is_not_excluded_for_own_configuration);
	UT_RUN(original_transport_position_is_preserved);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
