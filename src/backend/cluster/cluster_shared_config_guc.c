/*-------------------------------------------------------------------------
 * PGRAC: native policy for root-selected configuration entries.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "catalog/pg_authid_d.h"
#include "cluster/cluster_shared_config.h"
#include "mb/pg_wchar.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"

#define POLICY_COMMON 1
#define POLICY_INSTANCE 2
#define POLICY_COLD 4
#define POLICY_STRING 8
#define POLICY_PATH 16
#define POLICY_UUID 32

/* Native scalar defaults are common unless explicitly classified here.
 * Strings require an explicit entry: never persist arbitrary command strings,
 * connection strings, credentials or extension-loading instructions.
 * This classifies parameters, not a complete deployment profile. In particular
 * paths do not prove their contents, capacities do not prove recovery minima,
 * and a valid feature value does not prove its activation prerequisites.
 */
static const struct {
	const char *name;
	unsigned policy;
} config_policies[] = {
	{ "cluster.node_id", POLICY_INSTANCE | POLICY_COLD },
	{ "cluster.external_fence_socket_path", POLICY_INSTANCE | POLICY_STRING | POLICY_PATH },
	{ "cluster.config_file", POLICY_COMMON | POLICY_COLD | POLICY_STRING | POLICY_PATH },
	{ "cluster.shared_data_dir", POLICY_COMMON | POLICY_COLD | POLICY_STRING | POLICY_PATH },
	{ "cluster.wal_threads_dir", POLICY_COMMON | POLICY_COLD | POLICY_STRING | POLICY_PATH },
	{ "cluster.undo_tablespace_path", POLICY_COMMON | POLICY_COLD | POLICY_STRING | POLICY_PATH },
	{ "cluster.shared_storage_uuid", POLICY_COMMON | POLICY_COLD | POLICY_STRING | POLICY_UUID },
	{ "cluster.shared_storage_backend", POLICY_COMMON | POLICY_COLD },
	{ "cluster.controlfile_shared_authority", POLICY_COMMON | POLICY_COLD },
	{ "cluster.shared_catalog", POLICY_COMMON | POLICY_COLD },
	{ "cluster.shared_config", POLICY_COMMON | POLICY_COLD },
	{ "cluster.undo_gcs_coherence", POLICY_COMMON | POLICY_COLD },
	{ "cluster.enabled", POLICY_COMMON | POLICY_COLD },
	{ "port", POLICY_INSTANCE },
	{ "listen_addresses", POLICY_INSTANCE | POLICY_STRING },
	{ "unix_socket_directories", POLICY_INSTANCE | POLICY_STRING },
	{ "unix_socket_group", POLICY_INSTANCE | POLICY_STRING },
	{ "unix_socket_permissions", POLICY_INSTANCE },
	{ "log_directory", POLICY_INSTANCE | POLICY_STRING | POLICY_PATH },
	{ "log_filename", POLICY_INSTANCE | POLICY_STRING },
	{ "logging_collector", POLICY_INSTANCE },
	{ "log_destination", POLICY_INSTANCE | POLICY_STRING },
	{ "log_line_prefix", POLICY_INSTANCE | POLICY_STRING },
	{ "shared_buffers", POLICY_INSTANCE },
	{ "work_mem", POLICY_COMMON },
	{ "maintenance_work_mem", POLICY_COMMON },
	{ "temp_buffers", POLICY_COMMON },
	{ "datestyle", POLICY_COMMON | POLICY_STRING },
	{ "timezone", POLICY_COMMON | POLICY_STRING },
	{ "log_timezone", POLICY_COMMON | POLICY_STRING },
	{ "default_text_search_config", POLICY_COMMON | POLICY_STRING }
};

static void
policy_clear(ClusterSharedConfigPolicyReport *report)
{
	memset(report, 0, sizeof(*report));
	report->node_id = CLUSTER_SHARED_CONFIG_COMMON;
}

static ClusterControlRootResult
policy_refuse(ClusterSharedConfigPolicyReport *report, const ClusterSharedConfigEntry *entry,
			  ClusterSharedConfigPolicyReason reason)
{
	report->reason = reason;
	if (entry != NULL) {
		report->node_id = entry->node_id;
		if (entry->name != NULL)
			strlcpy(report->name, entry->name, sizeof(report->name));
	}
	return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
}

static bool
policy_path_valid(const char *value)
{
	const char *p;
	if (*value != '/' || strlen(value) >= MAXPGPATH)
		return false;
	p = value;
	while (*p) {
		const char *end;
		while (*p == '/')
			++p;
		end = p;
		while (*end && *end != '/')
			++end;
		if ((end - p == 1 && *p == '.') || (end - p == 2 && p[0] == '.' && p[1] == '.'))
			return false;
		p = end;
	}
	return true;
}

static bool
policy_uuid_valid(const char *value)
{
	bool nonzero = false;
	if (strlen(value) != 36)
		return false;
	for (size_t i = 0; i < 36; ++i) {
		char c = value[i];
		if (i == 8 || i == 13 || i == 18 || i == 23) {
			if (c != '-')
				return false;
		} else {
			if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
				return false;
			nonzero |= c != '0';
		}
	}
	return nonzero;
}

static ClusterControlRootResult
policy_check(const ClusterSharedConfigEntry *entry, bool online_change,
			 ClusterSharedConfigPolicyReport *report)
{
	struct config_generic *record;
	unsigned policy = POLICY_COMMON;
	size_t len;
	bool component_start = true;

	if (entry == NULL || entry->name == NULL || entry->value == NULL)
		return policy_refuse(report, NULL, CLUSTER_CONFIG_POLICY_FORMAT);
	len = strnlen(entry->name, CLUSTER_SHARED_CONFIG_MAX_NAME + 1);
	if (len == 0 || len > CLUSTER_SHARED_CONFIG_MAX_NAME)
		return policy_refuse(report, NULL, CLUSTER_CONFIG_POLICY_FORMAT);
	for (size_t i = 0; i < len; ++i) {
		char c = entry->name[i];
		if (c >= 'a' && c <= 'z')
			component_start = false;
		else if (!component_start && ((c >= '0' && c <= '9') || c == '_'))
			continue;
		else if (!component_start && c == '.')
			component_start = true;
		else
			return policy_refuse(report, NULL, CLUSTER_CONFIG_POLICY_FORMAT);
	}
	len = strnlen(entry->value, CLUSTER_SHARED_CONFIG_MAX_VALUE + 1);
	if (component_start || len > CLUSTER_SHARED_CONFIG_MAX_VALUE
		|| pg_encoding_verifymbstr(PG_UTF8, entry->value, len) != (int)len)
		return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_FORMAT);
	for (size_t i = 0; i < len; ++i)
		if ((unsigned char)entry->value[i] < 32 || entry->value[i] == 127)
			return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_FORMAT);
	if (entry->node_id < -1 || entry->node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_SCOPE);

	/* No placeholders, alias translation or loading extensions to resolve a
	 * name. Registry ownership is established before any native check hook.
	 */
	record = find_option(entry->name, false, true, DEBUG1);
	if (record == NULL || (record->flags & GUC_CUSTOM_PLACEHOLDER)
		|| pg_strcasecmp(record->name, entry->name) != 0)
		return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_UNKNOWN);
	if (record->context == PGC_INTERNAL || (record->flags & GUC_DISALLOW_IN_FILE))
		return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_CONTEXT);
	if ((strchr(entry->name, '.') && strncmp(entry->name, "cluster.", 8) != 0)
		|| strcmp(entry->name, "cluster.injection_points") == 0
		|| strncmp(entry->name, "cluster.test_", 13) == 0
		|| strncmp(entry->name, "cluster.gcs_block_drop_", 23) == 0)
		return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_UNSUPPORTED);
	for (size_t i = 0; i < lengthof(config_policies); ++i)
		if (strcmp(entry->name, config_policies[i].name) == 0) {
			policy = config_policies[i].policy;
			break;
		}
	if (record->vartype == PGC_STRING && !(policy & POLICY_STRING))
		return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_UNSUPPORTED);
	if ((entry->node_id == CLUSTER_SHARED_CONFIG_COMMON && !(policy & POLICY_COMMON))
		|| (entry->node_id != CLUSTER_SHARED_CONFIG_COMMON && !(policy & POLICY_INSTANCE)))
		return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_SCOPE);
	if (online_change && (policy & POLICY_COLD))
		return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_COLD_ONLY);
	if (((policy & POLICY_PATH) && !policy_path_valid(entry->value))
		|| ((policy & POLICY_UUID) && !policy_uuid_valid(entry->value)))
		return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_REFERENCE);

	/* Native parsing/ranges/enums/check hooks, including cross-GUC safety hooks.
	 * changeVal=false does not assign, push a GUC stack, set pending_restart or
	 * alter reset/source. Check hooks can inspect current process globals; this
	 * is not proof of a simultaneously applicable new deployment profile.
	 * No raw FDs/CF lock are owned here. ERROR from a hook propagates normally.
	 */
	if (set_config_option_ext(entry->name, entry->value, PGC_POSTMASTER, PGC_S_FILE,
							  BOOTSTRAP_SUPERUSERID, GUC_ACTION_SET, false, DEBUG1, false)
		!= -1)
		return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_VALUE);
	if (strcmp(entry->name, "cluster.node_id") == 0) {
		int node_id;
		if (!parse_int(entry->value, &node_id, 0, NULL) || node_id != entry->node_id)
			return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_SCOPE);
	}
	++report->checked_entries;
	if (record->context == PGC_POSTMASTER)
		++report->restart_entries;
	if (policy & POLICY_COLD)
		++report->cold_entries;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_shared_config_check_entry(const ClusterSharedConfigEntry *entry, bool online_change,
								  ClusterSharedConfigPolicyReport *report)
{
	if (report == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	policy_clear(report);
	return policy_check(entry, online_change, report);
}

typedef struct ConfigPolicyContext {
	ClusterSharedConfigPolicyReport *report;
	const ClusterSharedConfigRef *ref;
} ConfigPolicyContext;

static ClusterControlRootResult
policy_visit(const ClusterSharedConfigEntry *entry, void *arg)
{
	ConfigPolicyContext *context = arg;
	ClusterControlRootResult result = policy_check(entry, false, context->report);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (strcmp(entry->name, "cluster.shared_storage_uuid") == 0) {
		static const char hex[] = "0123456789abcdef";
		char expected[37];
		size_t pos = 0;
		for (size_t i = 0; i < 16; ++i) {
			uint8 byte = context->ref->identity.storage_uuid[i];
			if (i == 4 || i == 6 || i == 8 || i == 10)
				expected[pos++] = '-';
			expected[pos++] = hex[byte >> 4];
			expected[pos++] = hex[byte & 15];
		}
		expected[pos] = '\0';
		if (strcmp(expected, entry->value) != 0)
			return policy_refuse(context->report, entry, CLUSTER_CONFIG_POLICY_REFERENCE);
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_shared_config_check_gucs(const char *bytes, size_t len, const ClusterSharedConfigRef *ref,
								 ClusterSharedConfigPolicyReport *report)
{
	ClusterControlRootResult result;
	ConfigPolicyContext context = { report, ref };
	if (report == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	policy_clear(report);
	result = cluster_shared_config_visit(bytes, len, ref, policy_visit, &context);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY && report->reason == CLUSTER_CONFIG_POLICY_OK)
		report->reason = CLUSTER_CONFIG_POLICY_FORMAT;
	return result;
}

ClusterControlRootResult
cluster_shared_config_prepare_gucs(const char *shared_root, const char *bytes, size_t len,
								   const ClusterSharedConfigRef *ref,
								   const uint8 operation_uuid[16], ClusterSharedConfigStage *out,
								   ClusterSharedConfigPolicyReport *report)
{
	ClusterControlRootResult result;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (report != NULL)
		policy_clear(report);
	if (out == NULL || report == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_shared_config_check_gucs(bytes, len, ref, report);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	return cluster_shared_config_prepare(shared_root, bytes, len, ref, operation_uuid, out);
}
