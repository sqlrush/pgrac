/*-------------------------------------------------------------------------
 * pgrac_fenced_cib.c
 *    Read-only native Pacemaker configuration observation.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_cib.c
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include <limits.h>
#include <time.h>
#include "common/cryptohash.h"
#include "pgrac_fenced_cib.h"

#ifdef USE_PACEMAKER
#include <crm/cib.h>
#include <libxml/c14n.h>

#define CIB_MAX_BYTES (4 * 1024 * 1024)
#define CIB_MAX_NODES 32768
#define CIB_MAX_DEPTH 64

static bool
remaining_seconds(uint64 deadline, int *seconds)
{
	struct timespec now;
	uint64 current, remaining;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 || now.tv_nsec < 0
		|| now.tv_nsec >= 1000000000
		|| (uint64)now.tv_sec > (UINT64_MAX - UINT64_C(999999999)) / UINT64_C(1000000000))
		return false;
	current = (uint64)now.tv_sec * UINT64_C(1000000000) + now.tv_nsec;
	if (current >= deadline)
		return false;
	remaining = (deadline - current) / UINT64_C(1000000000);
	if ((deadline - current) % UINT64_C(1000000000) != 0)
		remaining++;
	*seconds = remaining > INT_MAX ? INT_MAX : (int)remaining;
	return true;
}

static bool
count_text(const xmlChar *text, size_t *remaining)
{
	size_t length = text == NULL ? 0 : strlen((const char *)text);
	if (length > *remaining)
		return false;
	*remaining -= length;
	return true;
}

static bool
bounded_tree(const xmlNode *first, unsigned depth, unsigned *nodes, size_t *bytes)
{
	if (depth > CIB_MAX_DEPTH)
		return false;
	for (const xmlNode *node = first; node != NULL; node = node->next) {
		if (*nodes == 0 || node->ns != NULL || node->nsDef != NULL
			|| (node->type != XML_ELEMENT_NODE && node->type != XML_TEXT_NODE
				&& node->type != XML_COMMENT_NODE)
			|| !count_text(node->name, bytes) || !count_text(node->content, bytes))
			return false;
		(*nodes)--;
		for (const xmlAttr *attr = node->properties; attr != NULL; attr = attr->next) {
			if (*nodes == 0 || attr->ns != NULL || !count_text(attr->name, bytes))
				return false;
			(*nodes)--;
			if (!bounded_tree(attr->children, depth + 1, nodes, bytes))
				return false;
		}
		if (!bounded_tree(node->children, depth + 1, nodes, bytes))
			return false;
	}
	return true;
}

static bool
attribute_u64(xmlNode *node, const char *name, uint64 *out)
{
	xmlChar *text = xmlGetProp(node, (const xmlChar *)name);
	uint64 result = 0;
	bool ok = text != NULL && text[0] != '\0' && (text[0] != '0' || text[1] == '\0');
	for (size_t n = 0; ok && text[n] != '\0'; n++) {
		uint32 digit = text[n] - '0';
		if (digit > 9 || result > (UINT64_MAX - digit) / 10)
			ok = false;
		else
			result = result * 10 + digit;
	}
	xmlFree(text);
	if (ok)
		*out = result;
	return ok;
}

static bool
configuration_digest(xmlNode *configuration, uint8 out[32])
{
	static const uint8 domain[] = "PGRAC-NATIVE-CIB-CONFIGURATION-V1";
	xmlDoc *copy = xmlNewDoc((const xmlChar *)"1.0");
	xmlNode *node = NULL;
	xmlChar *canonical = NULL;
	pg_cryptohash_ctx *hash = NULL;
	int length = -1;
	bool ok = false;
	if (copy == NULL)
		return false;
	node = xmlDocCopyNode(configuration, copy, 1);
	if (node != NULL) {
		xmlDocSetRootElement(copy, node);
		length = xmlC14NDocDumpMemory(copy, NULL, XML_C14N_1_0, NULL, 0, &canonical);
	}
	if (length > 0 && length <= CIB_MAX_BYTES && canonical != NULL)
		hash = pg_cryptohash_create(PG_SHA256);
	if (hash != NULL)
		ok = pg_cryptohash_init(hash) >= 0
			 && pg_cryptohash_update(hash, domain, sizeof(domain)) >= 0
			 && pg_cryptohash_update(hash, canonical, length) >= 0
			 && pg_cryptohash_final(hash, out, 32) >= 0;
	pg_cryptohash_free(hash);
	xmlFree(canonical);
	xmlFreeDoc(copy);
	return ok;
}

static bool
snapshot(xmlNode *root, PgracFencedCibObservation *out)
{
	xmlNode *configuration = NULL;
	unsigned nodes = CIB_MAX_NODES;
	size_t bytes = CIB_MAX_BYTES;
	if (root == NULL || root->doc == NULL || root->doc->intSubset != NULL
		|| root->doc->extSubset != NULL || xmlDocGetRootElement(root->doc) != root
		|| root->type != XML_ELEMENT_NODE || !xmlStrEqual(root->name, (const xmlChar *)"cib")
		|| !bounded_tree(root, 0, &nodes, &bytes)
		|| !attribute_u64(root, "admin_epoch", &out->admin_epoch)
		|| !attribute_u64(root, "epoch", &out->epoch)
		|| !attribute_u64(root, "num_updates", &out->num_updates))
		return false;
	for (xmlNode *node = root->children; node != NULL; node = node->next) {
		if (node->type == XML_ELEMENT_NODE
			&& xmlStrEqual(node->name, (const xmlChar *)"configuration")) {
			if (configuration != NULL)
				return false;
			configuration = node;
		}
	}
	return configuration != NULL && configuration_digest(configuration, out->configuration_digest);
}

static bool
name_valid(const char name[PGRAC_CIB_NAME_BYTES])
{
	size_t length = strnlen(name, PGRAC_CIB_NAME_BYTES);
	if (length == 0 || length == PGRAC_CIB_NAME_BYTES)
		return false;
	for (size_t n = 0; n < length; n++) {
		unsigned char ch = name[n];
		bool alnum
			= (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9');
		if (!alnum && (n == 0 || (ch != '-' && ch != '_' && ch != '.')))
			return false;
	}
	return true;
}

static bool
policy_valid(const PgracFencedCibPolicy *policy)
{
	static const uint8 zero[32] = { 0 };
	if (policy == NULL || !name_valid(policy->resource) || policy->nodes == NULL
		|| policy->node_count == 0 || policy->node_count > PGRAC_CIB_POLICY_MAX_NODES
		|| memcmp(policy->configuration_digest, zero, 32) == 0)
		return false;
	for (unsigned n = 0; n < policy->node_count; n++) {
		if (!name_valid(policy->nodes[n].name)
			|| memcmp(policy->nodes[n].guest_uuid, zero, 16) == 0)
			return false;
		for (unsigned prior = 0; prior < n; prior++)
			if (strcmp(policy->nodes[n].name, policy->nodes[prior].name) == 0
				|| memcmp(policy->nodes[n].guest_uuid, policy->nodes[prior].guest_uuid, 16) == 0)
				return false;
	}
	return true;
}

static bool
named(const xmlNode *node, const char *name)
{
	return node->type == XML_ELEMENT_NODE && xmlStrEqual(node->name, (const xmlChar *)name);
}

static bool
property_is(xmlNode *node, const char *name, const char *value, bool optional)
{
	xmlChar *actual = xmlGetProp(node, (const xmlChar *)name);
	bool result = actual == NULL ? optional : xmlStrEqual(actual, (const xmlChar *)value);
	xmlFree(actual);
	return result;
}

static xmlNode *
single_child(xmlNode *parent, const char *name)
{
	xmlNode *result = NULL;
	if (parent == NULL)
		return NULL;
	for (xmlNode *node = parent->children; node != NULL; node = node->next)
		if (named(node, name)) {
			if (result != NULL)
				return NULL;
			result = node;
		}
	return result;
}

/* The selected deployment uses literal policy. Do not partially interpret
 * general rule/template/reference semantics as though they were unconditional.
 * snapshot() has already bounded this tree's depth, nodes and bytes.
 */
static bool
literal_tree(xmlNode *first)
{
	for (xmlNode *node = first; node != NULL; node = node->next) {
		if (node->type != XML_ELEMENT_NODE)
			continue;
		if (named(node, "rule") || named(node, "template") || named(node, "fencing-topology")
			|| xmlHasProp(node, (const xmlChar *)"id-ref") != NULL
			|| xmlHasProp(node, (const xmlChar *)"template") != NULL
			|| !literal_tree(node->children))
			return false;
	}
	return true;
}

typedef struct CibPairs {
	unsigned count;
	xmlChar *names[128];
	xmlChar *values[128];
} CibPairs;

static void
free_pairs(CibPairs *pairs)
{
	for (unsigned n = 0; n < pairs->count; n++) {
		xmlFree(pairs->names[n]);
		xmlFree(pairs->values[n]);
	}
}

static bool
read_pairs(xmlNode *set, CibPairs *pairs)
{
	memset(pairs, 0, sizeof(*pairs));
	if (set == NULL)
		return false;
	for (xmlNode *node = set->children; node != NULL; node = node->next) {
		unsigned at;
		if (node->type != XML_ELEMENT_NODE)
			continue;
		if (!named(node, "nvpair") || pairs->count == lengthof(pairs->names))
			return false;
		at = pairs->count++;
		pairs->names[at] = xmlGetProp(node, (const xmlChar *)"name");
		pairs->values[at] = xmlGetProp(node, (const xmlChar *)"value");
		if (pairs->names[at] == NULL || pairs->names[at][0] == '\0' || pairs->values[at] == NULL
			|| pairs->values[at][0] == '\0')
			return false;
		for (unsigned n = 0; n < at; n++)
			if (xmlStrEqual(pairs->names[n], pairs->names[at]))
				return false;
		for (xmlNode *child = node->children; child != NULL; child = child->next)
			if (child->type == XML_ELEMENT_NODE)
				return false;
	}
	return true;
}

static const char *
pair_value(const CibPairs *pairs, const char *key)
{
	for (unsigned n = 0; n < pairs->count; n++)
		if (xmlStrEqual(pairs->names[n], (const xmlChar *)key))
			return (const char *)pairs->values[n];
	return NULL;
}

static bool
pair_is(const CibPairs *pairs, const char *key, const char *value, bool optional)
{
	const char *actual = pair_value(pairs, key);
	return actual == NULL ? optional : strcmp(actual, value) == 0;
}

static bool
cluster_policy(xmlNode *configuration)
{
	CibPairs pairs;
	bool ok = read_pairs(
		single_child(single_child(configuration, "crm_config"), "cluster_property_set"), &pairs);
	ok = ok && pair_is(&pairs, "stonith-enabled", "true", false)
		 && pair_is(&pairs, "stonith-action", "off", false)
		 && pair_is(&pairs, "no-quorum-policy", "freeze", false)
		 && pair_is(&pairs, "maintenance-mode", "false", false);
	free_pairs(&pairs);
	return ok;
}

static int
expected_node(const PgracFencedCibPolicy *policy, const char *name)
{
	if (name != NULL)
		for (unsigned n = 0; n < policy->node_count; n++)
			if (strcmp(name, policy->nodes[n].name) == 0)
				return n;
	return -1;
}

static bool
node_inventory(xmlNode *configuration, const PgracFencedCibPolicy *policy)
{
	xmlNode *nodes = single_child(configuration, "nodes");
	bool seen[PGRAC_CIB_POLICY_MAX_NODES] = { false };
	unsigned count = 0;
	if (nodes == NULL)
		return false;
	for (xmlNode *node = nodes->children; node != NULL; node = node->next) {
		xmlChar *name, *id;
		int index;
		bool ok;
		if (node->type != XML_ELEMENT_NODE)
			continue;
		name = xmlGetProp(node, (const xmlChar *)"uname");
		id = xmlGetProp(node, (const xmlChar *)"id");
		index = expected_node(policy, (const char *)name);
		ok = named(node, "node") && index >= 0 && !seen[index] && id != NULL && id[0] != '\0'
			 && property_is(node, "type", "member", true);
		for (xmlNode *prior = nodes->children; ok && prior != node; prior = prior->next)
			if (prior->type == XML_ELEMENT_NODE
				&& property_is(prior, "id", (const char *)id, false))
				ok = false;
		xmlFree(name);
		xmlFree(id);
		if (!ok)
			return false;
		seen[index] = true;
		count++;
	}
	return count == policy->node_count;
}

static void
guest_text(const uint8 uuid[16], char text[37])
{
	static const char hex[] = "0123456789abcdef";
	unsigned at = 0;
	for (unsigned n = 0; n < 16; n++) {
		if (n == 4 || n == 6 || n == 8 || n == 10)
			text[at++] = '-';
		text[at++] = hex[uuid[n] >> 4];
		text[at++] = hex[uuid[n] & 15];
	}
	text[at] = '\0';
}

static bool
host_bindings(const char *raw, const PgracFencedCibPolicy *policy, bool mapping)
{
	bool seen[PGRAC_CIB_POLICY_MAX_NODES] = { false };
	char *copy, *part, *cursor;
	unsigned count = 0;
	bool ok = raw != NULL && raw[0] != '\0';
	if (!ok || (copy = strdup(raw)) == NULL)
		return false;
	cursor = copy;
	while ((part = strsep(&cursor, mapping ? ";" : " ,\t\n")) != NULL) {
		char *uuid = mapping ? strchr(part, ':') : NULL;
		char wanted[37];
		int index;
		if (!mapping && part[0] == '\0')
			continue;
		if (uuid != NULL)
			*uuid++ = '\0';
		index = expected_node(policy, part);
		if (index < 0 || seen[index] || (mapping && uuid == NULL)) {
			ok = false;
			break;
		}
		if (mapping) {
			guest_text(policy->nodes[index].guest_uuid, wanted);
			if (strcmp(uuid, wanted) != 0) {
				ok = false;
				break;
			}
		}
		seen[index] = true;
		count++;
	}
	free(copy);
	return ok && count == policy->node_count;
}

static void
count_fencers(xmlNode *first, xmlNode **fencer, unsigned *count)
{
	for (xmlNode *node = first; node != NULL; node = node->next) {
		if (named(node, "primitive") && property_is(node, "class", "stonith", false)) {
			*fencer = node;
			(*count)++;
		}
		count_fencers(node->children, fencer, count);
	}
}

static bool
resource_policy(xmlNode *configuration, const PgracFencedCibPolicy *policy)
{
	xmlNode *resources = single_child(configuration, "resources"), *fencer = NULL;
	CibPairs pairs;
	unsigned count = 0;
	bool ok;
	const char *missing;
	if (resources == NULL)
		return false;
	count_fencers(resources->children, &fencer, &count);
	if (count != 1 || fencer->parent != resources
		|| !property_is(fencer, "id", policy->resource, false)
		|| !property_is(fencer, "type", "fence_virsh", false)
		|| xmlHasProp(fencer, (const xmlChar *)"provider") != NULL)
		return false;
	for (xmlNode *child = fencer->children; child != NULL; child = child->next)
		if (child->type == XML_ELEMENT_NODE && !named(child, "instance_attributes")
			&& !named(child, "operations"))
			return false;
	ok = read_pairs(single_child(fencer, "instance_attributes"), &pairs);
	missing = pair_value(&pairs, "missing_as_off");
	ok = ok && pair_is(&pairs, "pcmk_host_check", "static-list", false)
		 && pair_is(&pairs, "pcmk_reboot_action", "off", false)
		 && pair_is(&pairs, "pcmk_off_action", "off", true)
		 && pair_is(&pairs, "pcmk_on_action", "on", true)
		 && pair_is(&pairs, "pcmk_host_argument", "port", true)
		 && (missing == NULL || strcmp(missing, "false") == 0 || strcmp(missing, "0") == 0)
		 && pair_value(&pairs, "action") == NULL && pair_value(&pairs, "port") == NULL
		 && pair_value(&pairs, "plug") == NULL
		 && host_bindings(pair_value(&pairs, "pcmk_host_list"), policy, false)
		 && host_bindings(pair_value(&pairs, "pcmk_host_map"), policy, true);
	free_pairs(&pairs);
	return ok;
}

static bool
policy_matches(xmlNode *root, const PgracFencedCibPolicy *policy,
			   const PgracFencedCibObservation *observation)
{
	xmlNode *configuration = single_child(root, "configuration");
	return memcmp(policy->configuration_digest, observation->configuration_digest, 32) == 0
		   && configuration != NULL && xmlHasProp(configuration, (const xmlChar *)"id-ref") == NULL
		   && xmlHasProp(configuration, (const xmlChar *)"template") == NULL
		   && literal_tree(configuration->children) && cluster_policy(configuration)
		   && node_inventory(configuration, policy) && resource_policy(configuration, policy);
}

static PgracFencedProviderResult
observe_native(uint64 deadline, const PgracFencedCibPolicy *policy, PgracFencedCibObservation *out)
{
	cib_t *api;
	xmlNode *root = NULL;
	PgracFencedCibObservation candidate = { 0 };
	bool connected = false, ok = false;
	int seconds;
	if (!remaining_seconds(deadline, &seconds))
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	/* Explicit native constructor: environment-selected shadow/file data cannot
	 * stand in for the live IPC source. This opens only a query connection.
	 */
	api = cib_native_new();
	if (api == NULL)
		return PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	if (api->variant != cib_native || api->cmds == NULL || api->cmds->signon == NULL
		|| api->cmds->query == NULL || api->cmds->signoff == NULL)
		goto done;
	api->call_timeout = seconds;
	if (api->cmds->signon(api, "pgrac-fenced-query", cib_query) != 0)
		goto done;
	connected = true;
	if (!remaining_seconds(deadline, &seconds))
		goto done;
	api->call_timeout = seconds;
	if (api->cmds->query(api, NULL, &root, cib_sync_call) == 0
		&& remaining_seconds(deadline, &seconds))
		ok = snapshot(root, &candidate)
			 && (policy == NULL || policy_matches(root, policy, &candidate));
done:
	if (root != NULL)
		free_xml(root);
	if (connected && api->cmds->signoff(api) != 0)
		ok = false;
	cib_delete(api);
	if (!ok || !remaining_seconds(deadline, &seconds))
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	*out = candidate;
	return PGRAC_FENCED_PROVIDER_OK;
}
#endif

PgracFencedProviderResult
pgrac_fenced_cib_observe(uint64 deadline_mono_ns, PgracFencedCibObservation *out)
{
	if (out == NULL)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	memset(out, 0, sizeof(*out));
#ifdef USE_PACEMAKER
	return observe_native(deadline_mono_ns, NULL, out);
#else
	(void)deadline_mono_ns;
	return PGRAC_FENCED_PROVIDER_UNAVAILABLE;
#endif
}

PgracFencedProviderResult
pgrac_fenced_cib_check(uint64 deadline_mono_ns, const PgracFencedCibPolicy *policy,
					   PgracFencedCibObservation *out)
{
	if (out == NULL)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	memset(out, 0, sizeof(*out));
#ifdef USE_PACEMAKER
	if (!policy_valid(policy))
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	return observe_native(deadline_mono_ns, policy, out);
#else
	(void)deadline_mono_ns;
	(void)policy;
	return PGRAC_FENCED_PROVIDER_UNAVAILABLE;
#endif
}
