/*-------------------------------------------------------------------------
 * test_pgrac_fenced_cib.c
 *    Native API boundary tests; no cluster or power operation is performed.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_fenced_cib.c
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include <errno.h>
#include <time.h>
#include "pgrac_fenced_cib.h"
#ifdef USE_PACEMAKER
#include <crm/cib.h>
#include <libxml/parser.h>
#endif
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#ifdef USE_PACEMAKER
static cib_t connection;
static cib_api_operations_t operations;
static unsigned created, connected, queried, disconnected, deleted;
static int connect_status, query_status, disconnect_status;
static bool no_connection, no_operations, expire_on_connect, expire_on_query;
static uint64 now_ns;
static const char *document;
static const char valid[] = "<cib admin_epoch='0' epoch='7' num_updates='42'><configuration>"
							"<crm_config><cluster_property_set id='properties'>"
							"<nvpair id='off' name='stonith-action' value='off'/>"
							"</cluster_property_set></crm_config></configuration><status/></cib>";

/* Deterministic clock boundary only, never linked into production. */
int
clock_gettime(clockid_t clock, struct timespec *value)
{
	UT_ASSERT(clock == CLOCK_MONOTONIC);
	value->tv_sec = now_ns / UINT64_C(1000000000);
	value->tv_nsec = now_ns % UINT64_C(1000000000);
	return 0;
}

static int
fake_signon(cib_t *cib, const char *name, enum cib_conn_type type)
{
	UT_ASSERT(cib == &connection && name != NULL);
	UT_ASSERT(type == cib_query);
	connected++;
	if (expire_on_connect)
		now_ns += UINT64_C(10000000000);
	return connect_status;
}

static int
fake_query(cib_t *cib, const char *section, xmlNode **output, int options)
{
	xmlDoc *parsed;
	UT_ASSERT(cib == &connection && section == NULL);
	UT_ASSERT_EQ(options, cib_sync_call);
	UT_ASSERT(cib->call_timeout > 0 && cib->call_timeout <= 2);
	queried++;
	parsed = document == NULL
				 ? NULL
				 : xmlReadMemory(document, strlen(document), NULL, NULL, XML_PARSE_NONET);
	*output = parsed == NULL ? NULL : xmlDocGetRootElement(parsed);
	if (expire_on_query)
		now_ns += UINT64_C(10000000000);
	return query_status;
}

static int
fake_signoff(cib_t *cib)
{
	UT_ASSERT(cib == &connection);
	disconnected++;
	return disconnect_status;
}

cib_t *
cib_native_new(void)
{
	created++;
	memset(&connection, 0, sizeof(connection));
	memset(&operations, 0, sizeof(operations));
	connection.variant = cib_native;
	connection.cmds = no_operations ? NULL : &operations;
	operations.signon = fake_signon;
	operations.query = fake_query;
	operations.signoff = fake_signoff;
	return no_connection ? NULL : &connection;
}

void
cib_delete(cib_t *cib)
{
	UT_ASSERT(cib == &connection);
	deleted++;
}

static void
reset(void)
{
	created = connected = queried = disconnected = deleted = 0;
	connect_status = query_status = disconnect_status = 0;
	no_connection = no_operations = expire_on_connect = expire_on_query = false;
	now_ns = UINT64_C(100000000000);
	document = valid;
}

static PgracFencedProviderResult
observe(PgracFencedCibObservation *out)
{
	return pgrac_fenced_cib_observe(now_ns + UINT64_C(2000000000), out);
}

static void
assert_refused(void)
{
	PgracFencedCibObservation out, zero = { 0 };
	memset(&out, 0x7f, sizeof(out));
	UT_ASSERT_NE(observe(&out), PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
}

UT_TEST(test_native_query_only_and_exact_cleanup)
{
	PgracFencedCibObservation out;
	uint8 zero[32] = { 0 };
	reset();
	UT_ASSERT_EQ(observe(&out), PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(out.admin_epoch, 0);
	UT_ASSERT_EQ(out.epoch, 7);
	UT_ASSERT_EQ(out.num_updates, 42);
	UT_ASSERT(memcmp(out.configuration_digest, zero, sizeof(zero)) != 0);
	UT_ASSERT_EQ(created, 1);
	UT_ASSERT_EQ(connected, 1);
	UT_ASSERT_EQ(queried, 1);
	UT_ASSERT_EQ(disconnected, 1);
	UT_ASSERT_EQ(deleted, 1);
}

UT_TEST(test_configuration_fingerprint_is_not_runtime_status)
{
	PgracFencedCibObservation first, next;
	reset();
	UT_ASSERT_EQ(observe(&first), PGRAC_FENCED_PROVIDER_OK);
	document = "<cib epoch='7' num_updates='43' admin_epoch='0'><configuration>"
			   "<crm_config><cluster_property_set id='properties'>"
			   "<nvpair value='off' name='stonith-action' id='off'/>"
			   "</cluster_property_set></crm_config></configuration><status changed='yes'/></cib>";
	UT_ASSERT_EQ(observe(&next), PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(next.num_updates, 43);
	UT_ASSERT(memcmp(first.configuration_digest, next.configuration_digest, 32) == 0);
	document = "<cib admin_epoch='0' epoch='7' num_updates='44'><configuration>"
			   "<crm_config><cluster_property_set id='properties'>"
			   "<nvpair id='off' name='stonith-action' value='reboot'/>"
			   "</cluster_property_set></crm_config></configuration><status/></cib>";
	UT_ASSERT_EQ(observe(&next), PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT(memcmp(first.configuration_digest, next.configuration_digest, 32) != 0);
}

UT_TEST(test_malformed_configuration_is_not_observed)
{
	const char *bad[] = {
		NULL,
		"<cib/>",
		"<other admin_epoch='0' epoch='1' num_updates='1'><configuration/></other>",
		"<cib admin_epoch='0' epoch='1' num_updates='1'><configuration/><configuration/></cib>",
		"<cib admin_epoch='0' epoch='1' num_updates='1'><status/></cib>",
		"<cib admin_epoch='-1' epoch='1' num_updates='1'><configuration/></cib>",
		"<cib admin_epoch='0' epoch='01' num_updates='1'><configuration/></cib>",
		"<cib admin_epoch='0' epoch='1' num_updates='18446744073709551616'><configuration/></cib>",
		"<cib admin_epoch='0' epoch='1' num_updates='1'><configuration xmlns='urn:wrong'/></cib>",
		"<!DOCTYPE cib [<!ENTITY secret 'hidden'>]><cib admin_epoch='0' epoch='1' num_updates='1'>"
		"<configuration>&secret;</configuration></cib>"
	};
	for (unsigned n = 0; n < lengthof(bad); n++) {
		reset();
		document = bad[n];
		assert_refused();
		UT_ASSERT_EQ(deleted, 1);
	}
}

UT_TEST(test_native_failure_never_publishes_partial_observation)
{
	reset();
	no_connection = true;
	assert_refused();
	UT_ASSERT_EQ(deleted, 0);
	reset();
	no_operations = true;
	assert_refused();
	UT_ASSERT_EQ(deleted, 1);
	reset();
	connect_status = -ENOTCONN;
	assert_refused();
	UT_ASSERT_EQ(queried, 0);
	UT_ASSERT_EQ(deleted, 1);
	for (int status = -1; status <= 1; status += 2) {
		reset();
		query_status = status;
		assert_refused();
		UT_ASSERT_EQ(disconnected, 1);
		UT_ASSERT_EQ(deleted, 1);
	}
	reset();
	disconnect_status = -EIO;
	assert_refused();
	UT_ASSERT_EQ(deleted, 1);
}

UT_TEST(test_original_deadline_covers_connect_query_and_publication)
{
	PgracFencedCibObservation out, zero = { 0 };
	reset();
	memset(&out, 0x7f, sizeof(out));
	UT_ASSERT_NE(pgrac_fenced_cib_observe(now_ns, &out), PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(created, 0);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	reset();
	expire_on_connect = true;
	assert_refused();
	UT_ASSERT_EQ(queried, 0);
	UT_ASSERT_EQ(disconnected, 1);
	reset();
	expire_on_query = true;
	assert_refused();
	UT_ASSERT_EQ(disconnected, 1);
	UT_ASSERT_EQ(deleted, 1);
}

UT_TEST(test_uint64_boundary_and_bad_output)
{
	PgracFencedCibObservation out;
	reset();
	document = "<cib admin_epoch='18446744073709551615' epoch='0' num_updates='0'>"
			   "<configuration/></cib>";
	UT_ASSERT_EQ(observe(&out), PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(out.admin_epoch, UINT64_MAX);
	UT_ASSERT_EQ(pgrac_fenced_cib_observe(now_ns + 1, NULL), PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
}

UT_TEST(test_native_tree_work_is_bounded)
{
	const size_t capacity = 5 * 1024 * 1024;
	char *raw = malloc(capacity);
	size_t used;
	UT_ASSERT(raw != NULL);
	if (raw == NULL)
		return;
	reset();
	used = snprintf(raw, capacity,
					"<cib admin_epoch='0' epoch='1' num_updates='1'>"
					"<configuration><values");
	/* Odd remaining node count reaches zero inside an attribute's text child. */
	for (unsigned n = 0; n < 16400; n++)
		used += snprintf(raw + used, capacity - used, " n%u='v'", n);
	snprintf(raw + used, capacity - used, "/></configuration></cib>");
	document = raw;
	assert_refused();
	reset();
	used
		= snprintf(raw, capacity, "<cib admin_epoch='0' epoch='1' num_updates='1'><configuration>");
	for (unsigned n = 0; n < 70; n++)
		used += snprintf(raw + used, capacity - used, "<n>");
	for (unsigned n = 0; n < 70; n++)
		used += snprintf(raw + used, capacity - used, "</n>");
	snprintf(raw + used, capacity - used, "</configuration></cib>");
	document = raw;
	assert_refused();
	reset();
	used
		= snprintf(raw, capacity, "<cib admin_epoch='0' epoch='1' num_updates='1'><configuration>");
	memset(raw + used, 'x', 4 * 1024 * 1024);
	used += 4 * 1024 * 1024;
	snprintf(raw + used, capacity - used, "</configuration></cib>");
	document = raw;
	assert_refused();
	free(raw);
	document = valid;
}
#else
UT_TEST(test_non_native_build_is_unavailable)
{
	PgracFencedCibObservation out, zero = { 0 };
	memset(&out, 0x7f, sizeof(out));
	UT_ASSERT_EQ(pgrac_fenced_cib_observe(UINT64_MAX, &out), PGRAC_FENCED_PROVIDER_UNAVAILABLE);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
}
#endif

int
main(void)
{
#ifdef USE_PACEMAKER
	UT_PLAN(7);
	UT_RUN(test_native_query_only_and_exact_cleanup);
	UT_RUN(test_configuration_fingerprint_is_not_runtime_status);
	UT_RUN(test_malformed_configuration_is_not_observed);
	UT_RUN(test_native_failure_never_publishes_partial_observation);
	UT_RUN(test_original_deadline_covers_connect_query_and_publication);
	UT_RUN(test_uint64_boundary_and_bad_output);
	UT_RUN(test_native_tree_work_is_bounded);
#else
	UT_PLAN(1);
	UT_RUN(test_non_native_build_is_unavailable);
#endif
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
