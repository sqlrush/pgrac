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

static PgracFencedProviderResult
observe_native(uint64 deadline, PgracFencedCibObservation *out)
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
		ok = snapshot(root, &candidate);
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
	return observe_native(deadline_mono_ns, out);
#else
	(void)deadline_mono_ns;
	return PGRAC_FENCED_PROVIDER_UNAVAILABLE;
#endif
}
