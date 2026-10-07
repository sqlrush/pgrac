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
#include <libxml/c14n.h>
#include <libxml/parser.h>
#include <openssl/evp.h>
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

static char *
policy_fixture(PgracFencedCibPolicy *policy, PgracFencedCibNode nodes[4])
{
	/* Independent xmllint C14N + openssl digest, not the production observer. */
	static const uint8 digest[32]
		= { 0x1c, 0x3a, 0x95, 0x2b, 0xd4, 0xfe, 0x77, 0xef, 0x23, 0xf0, 0xa5,
			0x79, 0xee, 0x0f, 0x17, 0xeb, 0x64, 0x00, 0x05, 0x47, 0xcd, 0x39,
			0x06, 0x52, 0xca, 0x5a, 0xaa, 0x1a, 0x1d, 0xe0, 0xc2, 0x7f };
	FILE *file = fopen(PGRAC_CIB_POLICY_FIXTURE, "rb");
	char *raw = calloc(1, 16384);
	size_t length;
	if (file == NULL || raw == NULL)
		abort();
	length = fread(raw, 1, 16383, file);
	if (ferror(file) || !feof(file) || length == 0)
		abort();
	fclose(file);
	memset(policy, 0, sizeof(*policy));
	memset(nodes, 0, sizeof(*nodes) * 4);
	strcpy(policy->resource, "fence");
	memcpy(policy->configuration_digest, digest, sizeof(digest));
	policy->nodes = nodes;
	policy->node_count = 4;
	for (unsigned n = 0; n < 4; n++) {
		snprintf(nodes[n].name, sizeof(nodes[n].name), "node%u", n);
		nodes[n].guest_uuid[15] = n + 1;
	}
	reset();
	document = raw;
	return raw;
}

/* Fixture producer only: recalculate a deliberately changed document's pin
 * using libxml/OpenSSL directly so a semantic negative cannot pass merely
 * because the configuration hash changed. Never calls production hashing.
 */
static void
repin_fixture(PgracFencedCibPolicy *policy)
{
	xmlDoc *source = xmlReadMemory(document, strlen(document), NULL, NULL, XML_PARSE_NONET);
	xmlNode *root = xmlDocGetRootElement(source), *configuration = root->children;
	xmlDoc *copy = xmlNewDoc((const xmlChar *)"1.0");
	xmlChar *canonical = NULL;
	EVP_MD_CTX *digest = EVP_MD_CTX_new();
	int length;
	unsigned size;
	while (configuration && !xmlStrEqual(configuration->name, (const xmlChar *)"configuration"))
		configuration = configuration->next;
	if (!configuration || !copy || !digest)
		abort();
	xmlDocSetRootElement(copy, xmlDocCopyNode(configuration, copy, 1));
	length = xmlC14NDocDumpMemory(copy, NULL, XML_C14N_1_0, NULL, 0, &canonical);
	if (length <= 0 || EVP_DigestInit_ex(digest, EVP_sha256(), NULL) != 1
		|| EVP_DigestUpdate(digest, "PGRAC-NATIVE-CIB-CONFIGURATION-V1", 34) != 1
		|| EVP_DigestUpdate(digest, canonical, length) != 1
		|| EVP_DigestFinal_ex(digest, policy->configuration_digest, &size) != 1 || size != 32)
		abort();
	EVP_MD_CTX_free(digest);
	xmlFree(canonical);
	xmlFreeDoc(copy);
	xmlFreeDoc(source);
}

static char *
replace_fixture(const char *raw, const char *old, const char *replacement)
{
	const char *found = strstr(raw, old);
	size_t prefix;
	char *result;
	if (found == NULL)
		abort();
	prefix = found - raw;
	result = malloc(strlen(raw) + strlen(replacement) + 1);
	if (result == NULL)
		abort();
	memcpy(result, raw, prefix);
	strcpy(result + prefix, replacement);
	strcpy(result + prefix + strlen(replacement), found + strlen(old));
	return result;
}

static void
check_policy(PgracFencedCibPolicy *policy, PgracFencedProviderResult expected)
{
	PgracFencedCibObservation out, zero = { 0 };
	memset(&out, 0x7f, sizeof(out));
	UT_ASSERT_EQ(pgrac_fenced_cib_check(now_ns + UINT64_C(2000000000), policy, &out), expected);
	if (expected == PGRAC_FENCED_PROVIDER_OK) {
		UT_ASSERT(memcmp(out.configuration_digest, policy->configuration_digest, 32) == 0);
		UT_ASSERT_EQ(out.epoch, 7);
	} else
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
}

UT_TEST(test_checked_policy_binds_pin_and_inventory_in_one_native_query)
{
	PgracFencedCibPolicy policy;
	PgracFencedCibNode nodes[4];
	char *raw = policy_fixture(&policy, nodes);
	check_policy(&policy, PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(queried, 1);
	UT_ASSERT_EQ(disconnected, 1);
	policy.configuration_digest[0] ^= 1;
	check_policy(&policy, PGRAC_FENCED_PROVIDER_UNKNOWN);
	policy.configuration_digest[0] ^= 1;
	nodes[0].guest_uuid[15] = 9;
	check_policy(&policy, PGRAC_FENCED_PROVIDER_UNKNOWN);
	nodes[0].guest_uuid[15] = 1;
	strcpy(nodes[0].name, "replacement");
	check_policy(&policy, PGRAC_FENCED_PROVIDER_UNKNOWN);
	free(raw);
}

UT_TEST(test_checked_policy_rejects_requalified_unsafe_or_ambiguous_policy)
{
	const char *changes[][2] = {
		{ "name=\"stonith-enabled\" value=\"true\"", "name=\"stonith-enabled\" value=\"false\"" },
		{ "name=\"stonith-action\" value=\"off\"", "name=\"stonith-action\" value=\"reboot\"" },
		{ "value=\"freeze\"", "value=\"ignore\"" },
		{ "name=\"maintenance-mode\" value=\"false\"", "name=\"maintenance-mode\" value=\"true\"" },
		{ "value=\"static-list\"", "value=\"dynamic-list\"" },
		{ "name=\"pcmk_reboot_action\" value=\"off\"", "name=\"pcmk_reboot_action\" value=\"on\"" },
		{ "</instance_attributes>",
		  "<nvpair name=\"pcmk_off_action\" value=\"on\"/></instance_attributes>" },
		{ "</instance_attributes>",
		  "<nvpair name=\"pcmk_on_action\" value=\"reboot\"/></instance_attributes>" },
		{ "</instance_attributes>",
		  "<nvpair name=\"missing_as_off\" value=\"1\"/></instance_attributes>" },
		{ "</instance_attributes>",
		  "<nvpair name=\"port\" value=\"other\"/></instance_attributes>" },
		{ "</instance_attributes>",
		  "<nvpair name=\"pcmk_host_argument\" value=\"none\"/></instance_attributes>" },
		{ "</instance_attributes>",
		  "<nvpair name=\"pcmk_host_check\" value=\"static-list\"/></instance_attributes>" },
		{ "<instance_attributes id=\"fence-attributes\">",
		  "<instance_attributes id=\"fence-attributes\" id-ref=\"elsewhere\">" },
		{ "</instance_attributes>", "<rule id=\"conditional\"/></instance_attributes>" },
		{ "</primitive><primitive id=\"storage\"",
		  "<meta_attributes><nvpair name=\"provides\" "
		  "value=\"unfencing\"/></meta_attributes></primitive><primitive id=\"storage\"" },
		{ "type=\"fence_virsh\"", "type=\"fence_virsh\" template=\"alternate\"" },
		{ "<constraints/>", "<constraints/><fencing-topology/>" },
		{ "</resources>",
		  "<template id=\"other\" class=\"stonith\" type=\"fence_virsh\"/></resources>" },
		{ "class=\"ocf\"", "class=\"stonith\"" },
		{ "value=\"node0 node1 node2 node3\"", "value=\"node0 node0 node2 node3\"" },
		{ "node1:00000000-0000-0000-0000-000000000002",
		  "node1:00000000-0000-0000-0000-000000000001" },
		{ "uname=\"node1\"", "uname=\"node0\"" },
		{ "<node id=\"4\" uname=\"node3\"/>", "" },
		{ "<nodes>", "<nodes><node id=\"5\" uname=\"unexpected\"/>" },
	};
	PgracFencedCibPolicy policy;
	PgracFencedCibNode nodes[4];
	char *raw = policy_fixture(&policy, nodes);
	for (size_t n = 0; n < lengthof(changes); n++) {
		char *changed = replace_fixture(raw, changes[n][0], changes[n][1]);
		document = changed;
		repin_fixture(&policy);
		check_policy(&policy, PGRAC_FENCED_PROVIDER_UNKNOWN);
		free(changed);
	}
	free(raw);
}

UT_TEST(test_checked_policy_accepts_explicit_safe_defaults_and_status_change)
{
	PgracFencedCibPolicy policy;
	PgracFencedCibNode nodes[4];
	char *raw = policy_fixture(&policy, nodes);
	char *changed = replace_fixture(
		raw, "</instance_attributes>",
		"<nvpair name=\"pcmk_off_action\" value=\"off\"/>"
		"<nvpair name=\"pcmk_on_action\" value=\"on\"/>"
		"<nvpair name=\"pcmk_host_argument\" value=\"port\"/>"
		"<nvpair name=\"missing_as_off\" value=\"false\"/></instance_attributes>");
	document = changed;
	repin_fixture(&policy);
	check_policy(&policy, PGRAC_FENCED_PROVIDER_OK);
	free(changed);
	document = raw;
	repin_fixture(&policy);
	changed = replace_fixture(raw, "<status/>", "<status><node_state id=\"1\"/></status>");
	document = changed;
	check_policy(&policy, PGRAC_FENCED_PROVIDER_OK);
	free(changed);
	/* Runtime status is not interpreted as configuration, even when it uses
	 * names that are deliberately unsupported in configuration policy.
	 */
	changed = replace_fixture(raw, "<status/>", "<status><rule id=\"runtime-only\"/></status>");
	document = changed;
	check_policy(&policy, PGRAC_FENCED_PROVIDER_OK);
	expire_on_query = true;
	check_policy(&policy, PGRAC_FENCED_PROVIDER_UNKNOWN);
	free(changed);
	free(raw);
}

UT_TEST(test_checked_policy_invalid_expectation_never_queries_native)
{
	PgracFencedCibPolicy policy;
	PgracFencedCibNode nodes[4];
	char *raw = policy_fixture(&policy, nodes);
	check_policy(NULL, PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	policy.node_count = 0;
	check_policy(&policy, PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	policy.node_count = PGRAC_CIB_POLICY_MAX_NODES + 1;
	check_policy(&policy, PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	policy.node_count = 4;
	memset(policy.resource, 'r', sizeof(policy.resource));
	check_policy(&policy, PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	strcpy(policy.resource, "fence");
	memset(nodes[0].name, 'n', sizeof(nodes[0].name));
	check_policy(&policy, PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	strcpy(nodes[0].name, "node0");
	nodes[0].guest_uuid[15] = 0;
	check_policy(&policy, PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	nodes[0].guest_uuid[15] = 1;
	nodes[1] = nodes[0];
	check_policy(&policy, PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	nodes[1].guest_uuid[15] = 2;
	check_policy(&policy, PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	strcpy(nodes[1].name, "node1");
	memset(policy.configuration_digest, 0, sizeof(policy.configuration_digest));
	check_policy(&policy, PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	UT_ASSERT_EQ(created, 0);
	free(raw);
}
#else
UT_TEST(test_non_native_build_is_unavailable)
{
	PgracFencedCibObservation out, zero = { 0 };
	memset(&out, 0x7f, sizeof(out));
	UT_ASSERT_EQ(pgrac_fenced_cib_observe(UINT64_MAX, &out), PGRAC_FENCED_PROVIDER_UNAVAILABLE);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT_EQ(pgrac_fenced_cib_check(UINT64_MAX, NULL, &out), PGRAC_FENCED_PROVIDER_UNAVAILABLE);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
}
#endif

int
main(void)
{
#ifdef USE_PACEMAKER
	UT_PLAN(11);
	UT_RUN(test_native_query_only_and_exact_cleanup);
	UT_RUN(test_configuration_fingerprint_is_not_runtime_status);
	UT_RUN(test_malformed_configuration_is_not_observed);
	UT_RUN(test_native_failure_never_publishes_partial_observation);
	UT_RUN(test_original_deadline_covers_connect_query_and_publication);
	UT_RUN(test_uint64_boundary_and_bad_output);
	UT_RUN(test_native_tree_work_is_bounded);
	UT_RUN(test_checked_policy_binds_pin_and_inventory_in_one_native_query);
	UT_RUN(test_checked_policy_rejects_requalified_unsafe_or_ambiguous_policy);
	UT_RUN(test_checked_policy_accepts_explicit_safe_defaults_and_status_change);
	UT_RUN(test_checked_policy_invalid_expectation_never_queries_native);
#else
	UT_PLAN(1);
	UT_RUN(test_non_native_build_is_unavailable);
#endif
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
