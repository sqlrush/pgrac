/* Author: SqlRush <sqlrush@gmail.com> */
/* Execute the production refusal routine and consumer cuts. Classification,
 * injection, dispatch and error reporting are controlled boundaries; no page
 * construction, real admission, transport, or native SQL is proved here. */
#include "postgres.h"
#include "cluster/cluster_cr_coordinator_stat.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_inject.h"
#include <setjmp.h>
#include <stdarg.h>
#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
int cluster_node_id = 1;
int cluster_cross_instance_cr_coordinator = CR_COORD_MODE_BOUNDARY;
bool cluster_cross_instance_cr_probe;
static jmp_buf refusal_jump;
static int saved_code, dispatch_calls, forward_logs;
static char saved_message[512], saved_detail[512], saved_hint[512];
static uint64 counters[CR_COORD_COUNTER__COUNT];
static const char *injection;
static ClusterCrCoordOriginClass origin_class;
static ClusterSemanticAdmissionResult dispatch_result;
static bool dispatch_fetched;
static ClusterR4SourceCrRequest saved_request;
static struct {
	pg_atomic_uint64 cr_remote_failed_count;
} cr_shared, *CRShared = &cr_shared;

int
errcode(int code)
{
	saved_code = code;
	return 0;
}

#define CAPTURE_ERROR_FIELD(name, destination) \
	int name(const char *format, ...) \
	{ \
		va_list args; \
		va_start(args, format); \
		vsnprintf(destination, sizeof(destination), format, args); \
		va_end(args); \
		return 0; \
	}
CAPTURE_ERROR_FIELD(errmsg, saved_message)
CAPTURE_ERROR_FIELD(errdetail, saved_detail)
CAPTURE_ERROR_FIELD(errhint, saved_hint)

#undef ereport
#define ereport(level, fields) do { (void)(fields); longjmp(refusal_jump, 1); } while (0)
#undef elog
#define elog(level, ...) do { forward_logs++; } while (0)

void
cluster_cr_coordinator_stat_bump(ClusterCrCoordCounter counter)
{
	counters[counter]++;
}

ClusterCrCoordOriginClass
cluster_cr_coordinator_classify_origin(NodeId origin)
{
	return origin_class;
}

NodeId
uba_origin_node_id(UBA uba)
{
	return 0;
}

bool
cluster_cr_injection_armed(const char *name, uint64 *param)
{
	if (injection == NULL || strcmp(injection, name) != 0)
		return false;
	*param = 7;
	return true;
}

ClusterSemanticAdmissionResult
cluster_r4_source_cr_dispatch(ClusterR4SourceCrOp op,
							  const ClusterR4SourceCrRequest *request,
							  ClusterR4SourceCrResult *result)
{
	Assert(op == CLUSTER_R4_SOURCE_CR_FETCH);
	dispatch_calls++;
	saved_request = *request;
	/* On non-OK admission the consumer must not inspect result bytes. */
	if (dispatch_result == CLUSTER_SEMANTIC_ADMISSION_OK) {
		result->fetched = dispatch_fetched;
		result->partial = false;
	}
	return dispatch_result;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d %s\n", file, line, condition);
	abort();
}

#include "test_cluster_cr_refusal.inc"

static void
reset_fixture(void)
{
	saved_code = dispatch_calls = 0;
	saved_message[0] = saved_detail[0] = saved_hint[0] = '\0';
	memset(counters, 0, sizeof(counters));
	injection = NULL;
	origin_class = CR_COORD_ORIGIN_RUNTIME_REMOTE;
	dispatch_result = CLUSTER_SEMANTIC_ADMISSION_OK;
	dispatch_fetched = false;
	cluster_cross_instance_cr_coordinator = CR_COORD_MODE_BOUNDARY;
	cluster_cross_instance_cr_probe = false;
	CRShared = &cr_shared;
	pg_atomic_init_u64(&cr_shared.cr_remote_failed_count, 0);
}

static void
expect_detail(const char *site, bool known, int admission)
{
	char expected[256];
	bool fetched_known = known && admission == CLUSTER_SEMANTIC_ADMISSION_OK;

	snprintf(expected, sizeof(expected),
			 "refusal_site=%s source_admission_known=%d source_admission=%d "
			 "source_fetched_known=%d source_fetched=%d",
			 site, known, admission, fetched_known, fetched_known ? 0 : -1);
	UT_ASSERT_EQ(saved_code, ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED);
	UT_ASSERT_STR_EQ(saved_detail, expected);
	UT_ASSERT_STR_CONTAINS(saved_message, "remote-undo-read leg");
	UT_ASSERT_STR_CONTAINS(saved_hint, "spec-5.57");
}

static void
source_dispatch(void)
{
	RelFileLocator locator = { 1663, 5, 16385 };
	static char page[BLCKSZ];

	fixture_source_dispatch(0, locator, MAIN_FORKNUM, 3, 7480, page);
}

UT_TEST(injection_site)
{
	reset_fixture();
	injection = "cr_cross_instance";
	if (setjmp(refusal_jump) == 0) {
		cr_check_error_injections();
		UT_ASSERT(false);
	}
	expect_detail("injection", false, -1);
	UT_ASSERT_STR_CONTAINS(saved_message, "origin_node_id=7, local=1");
	UT_ASSERT_EQ(dispatch_calls, 0);
}

UT_TEST(chain_walk_site)
{
	reset_fixture();
	if (setjmp(refusal_jump) == 0) {
		fixture_walk_origin((UBA){ 0 });
		UT_ASSERT(false);
	}
	expect_detail("chain_walk", false, -1);
	UT_ASSERT_EQ(dispatch_calls, 0);
}

UT_TEST(history_site_and_guards)
{
	bool partial = false;

	reset_fixture();
	fixture_history_origin(0, false, false, &partial);
	UT_ASSERT_EQ(saved_code, 0);
	UT_ASSERT(!partial);
	fixture_history_origin(0, true, true, &partial);
	UT_ASSERT(partial);
	UT_ASSERT_EQ(saved_code, 0);
	if (setjmp(refusal_jump) == 0) {
		fixture_history_origin(0, true, false, &partial);
		UT_ASSERT(false);
	}
	expect_detail("retained_history", false, -1);
}

static void
check_dispatch_refusal(ClusterSemanticAdmissionResult result)
{
	reset_fixture();
	dispatch_result = result;
	if (setjmp(refusal_jump) == 0) {
		source_dispatch();
		UT_ASSERT(false);
	}
	expect_detail("source_dispatch", true, (int)result);
	UT_ASSERT_EQ(dispatch_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&cr_shared.cr_remote_failed_count), 1);
	UT_ASSERT_EQ(saved_request.origin_node, 0);
	UT_ASSERT_EQ(saved_request.read_scn, 7480);
	UT_ASSERT_EQ(saved_request.tag.blockNum, 3);
	UT_ASSERT_NOT_NULL(saved_request.dst_page);
}

UT_TEST(source_dormant) { check_dispatch_refusal(CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT); }
UT_TEST(target_disabled) { check_dispatch_refusal(CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED); }
UT_TEST(generation_changed) { check_dispatch_refusal(CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED); }
UT_TEST(admission_closed) { check_dispatch_refusal(CLUSTER_SEMANTIC_ADMISSION_CLOSED); }
UT_TEST(admitted_not_fetched) { check_dispatch_refusal(CLUSTER_SEMANTIC_ADMISSION_OK); }
UT_TEST(unrecognized_admission) { check_dispatch_refusal((ClusterSemanticAdmissionResult)99); }

UT_TEST(success_and_local_origin)
{
	reset_fixture();
	dispatch_fetched = true;
	source_dispatch();
	UT_ASSERT_EQ(saved_code, 0);
	UT_ASSERT_EQ(dispatch_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&cr_shared.cr_remote_failed_count), 0);
	origin_class = CR_COORD_ORIGIN_OWN;
	fixture_walk_origin((UBA){ 0 });
	source_dispatch();
	UT_ASSERT_EQ(dispatch_calls, 1);
	UT_ASSERT_EQ(saved_code, 0);
	origin_class = CR_COORD_ORIGIN_MATERIALIZED_REMOTE;
	fixture_walk_origin((UBA){ 0 });
	UT_ASSERT_EQ(counters[CR_COORD_MATERIALIZED_REMOTE_SERVED], 1);
}

UT_TEST(counter_modes_and_forward_once)
{
	/* Each real refusal remains fatal in all GUC modes. */
	for (int mode = CR_COORD_MODE_OFF; mode <= CR_COORD_MODE_FORWARD; mode++) {
		reset_fixture();
		cluster_cross_instance_cr_coordinator = mode;
		cluster_cross_instance_cr_probe = true;
		if (setjmp(refusal_jump) == 0) {
			fixture_walk_origin((UBA){ 0 });
			UT_ASSERT(false);
		}
		expect_detail("chain_walk", false, -1);
		UT_ASSERT_EQ(counters[CR_COORD_CROSS_INSTANCE_CR_REFUSED], mode != CR_COORD_MODE_OFF);
		UT_ASSERT_EQ(counters[CR_COORD_REMOTE_UNDO_READ_REFUSED], mode != CR_COORD_MODE_OFF);
		UT_ASSERT_EQ(counters[CR_COORD_CROSS_INSTANCE_BOUNDARY_PROBE], 1);
	}
	if (setjmp(refusal_jump) == 0) {
		fixture_walk_origin((UBA){ 0 });
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(forward_logs, 1);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(injection_site);
	UT_RUN(chain_walk_site);
	UT_RUN(history_site_and_guards);
	UT_RUN(source_dormant);
	UT_RUN(target_disabled);
	UT_RUN(generation_changed);
	UT_RUN(admission_closed);
	UT_RUN(admitted_not_fetched);
	UT_RUN(unrecognized_admission);
	UT_RUN(success_and_local_origin);
	UT_RUN(counter_modes_and_forward_once);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
