/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_pacemaker.c
 *    Test-only native API linkage for the production Pacemaker action client.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_fenced_pacemaker.c
 *
 * NOTES
 *    PGRAC-original unit. It never connects to a fencer or changes guest power.
 *    Enabled tests use the distribution's real public Pacemaker API header.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <errno.h>
#include <time.h>

#include "pgrac_fenced_pacemaker.h"

#ifdef USE_PACEMAKER
#include <crm/stonith-ng.h>
#endif

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static PgracFencedPacemakerActionV1
request(void)
{
	PgracFencedPacemakerActionV1 action;
	struct timespec now;

	memset(&action, 0, sizeof(action));
	memcpy(action.node, "node-2", 7);
	memset(action.operation_id, 0x17, 16);
	action.attempt = 3;
	if (clock_gettime(CLOCK_MONOTONIC, &now) == 0)
		action.deadline_mono_ns
			= (uint64)now.tv_sec * UINT64_C(1000000000) + now.tv_nsec + UINT64_C(4000000000);
	return action;
}

#ifdef USE_PACEMAKER
static stonith_t connection;
static stonith_api_operations_t operations;
static unsigned int created, connected, fenced, disconnected, deleted;
static int connect_result, fence_result;
static char client_name[80];
static char target_name[64];
static char requested_action[8];
static int options_seen, timeout_seen, tolerance_seen;

static int
fake_connect(stonith_t *st, const char *name, int *fd)
{
	UT_ASSERT(st == &connection && fd != NULL);
	connected++;
	snprintf(client_name, sizeof(client_name), "%s", name);
	*fd = 91;
	return connect_result;
}

static int
fake_disconnect(stonith_t *st)
{
	UT_ASSERT(st == &connection);
	disconnected++;
	return 0;
}

static int
fake_fence(stonith_t *st, int options, const char *node, const char *action, int timeout,
		   int tolerance)
{
	UT_ASSERT(st == &connection);
	fenced++;
	options_seen = options;
	timeout_seen = timeout;
	tolerance_seen = tolerance;
	snprintf(target_name, sizeof(target_name), "%s", node);
	snprintf(requested_action, sizeof(requested_action), "%s", action);
	st->call_id = 7; /* Deliberately reused on every new connection. */
	return fence_result;
}

stonith_t *
stonith_api_new(void)
{
	created++;
	memset(&connection, 0, sizeof(connection));
	memset(&operations, 0, sizeof(operations));
	connection.cmds = &operations;
	operations.connect = fake_connect;
	operations.disconnect = fake_disconnect;
	operations.fence = fake_fence;
	return &connection;
}

void
stonith_api_delete(stonith_t *st)
{
	UT_ASSERT(st == &connection);
	deleted++;
}

static void
reset(void)
{
	created = connected = fenced = disconnected = deleted = 0;
	connect_result = fence_result = 0;
	memset(client_name, 0, sizeof(client_name));
}

UT_TEST(test_exact_action_new_connection_and_no_history_tolerance)
{
	PgracFencedPacemakerActionV1 action = request();
	PgracFencedPacemakerActionResultV1 out;
	char first_name[80];

	reset();
	UT_ASSERT_EQ(pgrac_fenced_pacemaker_action(&action, &out), PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(options_seen, st_opt_sync_call);
	UT_ASSERT_EQ(tolerance_seen, 0);
	UT_ASSERT(timeout_seen > 0 && timeout_seen <= 4);
	UT_ASSERT(strcmp(target_name, "node-2") == 0);
	UT_ASSERT(strcmp(requested_action, "off") == 0);
	UT_ASSERT(strstr(client_name, "17171717171717171717171717171717") != NULL);
	UT_ASSERT_EQ(out.native_status, 0);
	UT_ASSERT_EQ(out.call_id, 7);
	memcpy(first_name, client_name, sizeof(first_name));
	action.operation_id[0]++;
	action.turn_on = true;
	UT_ASSERT_EQ(pgrac_fenced_pacemaker_action(&action, &out), PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT(strcmp(first_name, client_name) != 0);
	UT_ASSERT(strcmp(requested_action, "on") == 0);
	UT_ASSERT_EQ(out.call_id, 7); /* Same local call ID does not merge operations. */
	UT_ASSERT_EQ(created, 2);
	UT_ASSERT_EQ(connected, 2);
	UT_ASSERT_EQ(fenced, 2);
	UT_ASSERT_EQ(disconnected, 2);
	UT_ASSERT_EQ(deleted, 2);
}

UT_TEST(test_native_errors_and_positive_call_id_are_not_sync_success)
{
	PgracFencedPacemakerActionV1 action = request();
	PgracFencedPacemakerActionResultV1 out;

	reset();
	connect_result = -ENOTCONN;
	UT_ASSERT_NE(pgrac_fenced_pacemaker_action(&action, &out), PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(fenced, 0);
	UT_ASSERT_EQ(deleted, 1);
	connect_result = 0;
	fence_result = -ETIMEDOUT;
	UT_ASSERT_EQ(pgrac_fenced_pacemaker_action(&action, &out), PGRAC_FENCED_PROVIDER_UNKNOWN);
	UT_ASSERT_EQ(out.native_status, -ETIMEDOUT);
	fence_result = 7;
	UT_ASSERT_EQ(pgrac_fenced_pacemaker_action(&action, &out), PGRAC_FENCED_PROVIDER_UNKNOWN);
	UT_ASSERT_EQ(out.native_status, 7);
	UT_ASSERT_EQ(fenced, 2);
	UT_ASSERT_EQ(deleted, 3);
}

UT_TEST(test_bad_identity_and_expired_deadline_have_zero_native_calls)
{
	PgracFencedPacemakerActionV1 action = request();
	PgracFencedPacemakerActionResultV1 out;

	reset();
	action.node[0] = '-';
	UT_ASSERT_EQ(pgrac_fenced_pacemaker_action(&action, &out), PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	action = request();
	memset(action.node, 'x', sizeof(action.node));
	UT_ASSERT_EQ(pgrac_fenced_pacemaker_action(&action, &out), PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	action = request();
	action.node[2] = ';';
	UT_ASSERT_EQ(pgrac_fenced_pacemaker_action(&action, &out), PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	action = request();
	memset(action.operation_id, 0, 16);
	UT_ASSERT_EQ(pgrac_fenced_pacemaker_action(&action, &out), PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	action = request();
	action.attempt = 0;
	UT_ASSERT_EQ(pgrac_fenced_pacemaker_action(&action, &out), PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	action = request();
	action.deadline_mono_ns = 1;
	UT_ASSERT_EQ(pgrac_fenced_pacemaker_action(&action, &out), PGRAC_FENCED_PROVIDER_UNKNOWN);
	UT_ASSERT_EQ(out.native_status, -ETIMEDOUT);
	UT_ASSERT_EQ(created, 0);
	UT_ASSERT_EQ(fenced, 0);
}
#else
UT_TEST(test_missing_native_api_is_unavailable)
{
	PgracFencedPacemakerActionV1 action = request();
	PgracFencedPacemakerActionResultV1 out;

	UT_ASSERT_EQ(pgrac_fenced_pacemaker_action(&action, &out), PGRAC_FENCED_PROVIDER_UNAVAILABLE);
}
#endif

int
main(void)
{
#ifdef USE_PACEMAKER
	UT_PLAN(3);
	UT_RUN(test_exact_action_new_connection_and_no_history_tolerance);
	UT_RUN(test_native_errors_and_positive_call_id_are_not_sync_success);
	UT_RUN(test_bad_identity_and_expired_deadline_have_zero_native_calls);
#else
	UT_PLAN(1);
	UT_RUN(test_missing_native_api_is_unavailable);
#endif
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
