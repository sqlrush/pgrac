/*-------------------------------------------------------------------------
 * PGRAC: native policy for root-selected configuration entries.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "catalog/pg_authid_d.h"
#include "cluster/cluster_shared_config.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/resowner.h"
#include "cluster_control_root_private.h"

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
policy_check(const ClusterSharedConfigEntry *entry, bool online_change, bool native_check,
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
	if (native_check
		&& set_config_option_ext(entry->name, entry->value, PGC_POSTMASTER, PGC_S_FILE,
								 BOOTSTRAP_SUPERUSERID, GUC_ACTION_SET, false, DEBUG1, false)
			   != -1)
		return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_VALUE);
	if (strcmp(entry->name, "cluster.node_id") == 0) {
		int node_id;
		if (!parse_int(entry->value, &node_id, 0, NULL))
			return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_VALUE);
		if (node_id != entry->node_id)
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
	return policy_check(entry, online_change, true, report);
}

typedef struct ConfigPolicyContext {
	ClusterSharedConfigPolicyReport *report;
	const ClusterSharedConfigRef *ref;
	bool native_check;
} ConfigPolicyContext;

static ClusterControlRootResult
policy_visit(const ClusterSharedConfigEntry *entry, void *arg)
{
	ConfigPolicyContext *context = arg;
	ClusterControlRootResult result
		= policy_check(entry, false, context->native_check, context->report);
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
	ConfigPolicyContext context = { report, ref, true };
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

/* PGRAC: minimum cold-profile identity bindings, not permission to activate.
 * Native value hooks run after actual assignment in the startup applier.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef enum ConfigBootstrapKind {
	CONFIG_BOOTSTRAP_TRUE,
	CONFIG_BOOTSTRAP_BACKEND,
	CONFIG_BOOTSTRAP_UUID,
	CONFIG_BOOTSTRAP_DATA,
	CONFIG_BOOTSTRAP_WAL,
	CONFIG_BOOTSTRAP_UNDO
} ConfigBootstrapKind;

static const struct {
	const char *name;
	ConfigBootstrapKind kind;
} bootstrap_required[] = { { "cluster.controlfile_shared_authority", CONFIG_BOOTSTRAP_TRUE },
						   { "cluster.enabled", CONFIG_BOOTSTRAP_TRUE },
						   { "cluster.merged_recovery", CONFIG_BOOTSTRAP_TRUE },
						   { "cluster.shared_catalog", CONFIG_BOOTSTRAP_TRUE },
						   { "cluster.shared_config", CONFIG_BOOTSTRAP_TRUE },
						   { "cluster.shared_data_dir", CONFIG_BOOTSTRAP_DATA },
						   { "cluster.shared_storage_backend", CONFIG_BOOTSTRAP_BACKEND },
						   { "cluster.shared_storage_uuid", CONFIG_BOOTSTRAP_UUID },
						   { "cluster.smgr_user_relations", CONFIG_BOOTSTRAP_TRUE },
						   { "cluster.undo_tablespace_path", CONFIG_BOOTSTRAP_UNDO },
						   { "cluster.wal_threads_dir", CONFIG_BOOTSTRAP_WAL } };

typedef struct ConfigBootstrapContext {
	ConfigPolicyContext policy;
	const char *paths[3];
	bool seen[lengthof(bootstrap_required)];
	uint64 nodes[2];
} ConfigBootstrapContext;

static bool
bootstrap_report_overlap(const void *input, size_t len, ClusterSharedConfigPolicyReport *report)
{
	uintptr_t a = (uintptr_t)input, b = (uintptr_t)report;
	if (input == NULL || len == 0)
		return false;
	return a <= b ? b - a < len : a - b < sizeof(*report);
}

static bool
bootstrap_config_path(const char *path)
{
	size_t len = path != NULL ? strnlen(path, MAXPGPATH) : 0;
	size_t start = 1;
	if (len < 2 || len >= MAXPGPATH || path[0] != '/')
		return false;
	for (size_t i = 1; i <= len; i++) {
		if (i == len || path[i] == '/') {
			size_t size = i - start;
			if (size == 0 || (size == 1 && path[start] == '.')
				|| (size == 2 && path[start] == '.' && path[start + 1] == '.'))
				return false;
			start = i + 1;
		}
	}
	return true;
}

ClusterControlRootResult
cluster_shared_config_check_recovery_capacity(const ClusterControlRecoveryCapacity *required,
											  ClusterSharedConfigPolicyReport *report)
{
	static const char *const names[]
		= { "max_connections", "max_worker_processes", "max_wal_senders",
			"max_prepared_transactions", "max_locks_per_transaction" };
	uint32 values[5];
	bool alias;

	if (report == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	alias = bootstrap_report_overlap(required, sizeof(*required), report);
	policy_clear(report);
	if (alias || required == NULL || required->current_sources == 0
		|| required->current_sources > CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| required->history_sources > required->current_sources * CLUSTER_WAL_HISTORY_MAX_RECORDS
		|| required->pending_sources > required->current_sources || required->max_connections == 0
		|| required->max_locks_per_xact == 0)
		return policy_refuse(report, NULL, CLUSTER_CONFIG_POLICY_FORMAT);
	values[0] = required->max_connections;
	values[1] = required->max_worker_processes;
	values[2] = required->max_wal_senders;
	values[3] = required->max_prepared_xacts;
	values[4] = required->max_locks_per_xact;
	for (size_t i = 0; i < lengthof(values); i++)
		if (values[i] > PG_INT32_MAX)
			return policy_refuse(report, NULL, CLUSTER_CONFIG_POLICY_FORMAT);
	for (size_t i = 0; i < lengthof(names); i++) {
		ClusterSharedConfigEntry entry = { CLUSTER_SHARED_CONFIG_COMMON, names[i], NULL };
		struct config_generic *record = find_option(names[i], false, true, DEBUG1);
		int actual;
		if (record == NULL || record->vartype != PGC_INT || record->context != PGC_POSTMASTER)
			return policy_refuse(report, &entry, CLUSTER_CONFIG_POLICY_CONTEXT);
		actual = *((struct config_int *)record)->variable;
		report->checked_entries++;
		if (actual < 0 || (uint32)actual < values[i])
			return policy_refuse(report, &entry, CLUSTER_CONFIG_POLICY_RECOVERY_CAPACITY);
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
bootstrap_config_visit(const ClusterSharedConfigEntry *entry, void *arg)
{
	ConfigBootstrapContext *context = arg;
	ClusterControlRootResult result = policy_visit(entry, &context->policy);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (entry->node_id != CLUSTER_SHARED_CONFIG_COMMON) {
		/* Static policy already verifies native scope and exact node number. */
		if (strcmp(entry->name, "cluster.node_id") == 0)
			context->nodes[entry->node_id / 64] |= UINT64CONST(1) << (entry->node_id % 64);
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	for (size_t i = 0; i < lengthof(bootstrap_required); i++) {
		ConfigBootstrapKind kind = bootstrap_required[i].kind;
		bool enabled;
		if (strcmp(entry->name, bootstrap_required[i].name) != 0)
			continue;
		if (kind == CONFIG_BOOTSTRAP_TRUE && (!parse_bool(entry->value, &enabled) || !enabled))
			return policy_refuse(context->policy.report, entry, CLUSTER_CONFIG_POLICY_VALUE);
		if ((kind == CONFIG_BOOTSTRAP_BACKEND && pg_strcasecmp(entry->value, "cluster_fs") != 0)
			|| (kind >= CONFIG_BOOTSTRAP_DATA
				&& strcmp(entry->value, context->paths[kind - CONFIG_BOOTSTRAP_DATA]) != 0))
			return policy_refuse(context->policy.report, entry, CLUSTER_CONFIG_POLICY_REFERENCE);
		/* UUID equality is checked by the shared production policy visitor. */
		context->seen[i] = true;
		break;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_shared_config_check_bootstrap(const char *bytes, size_t len,
									  const ClusterSharedConfigRef *ref, int node_id,
									  const char *shared_root, const char *wal_root,
									  const char *undo_root,
									  ClusterSharedConfigPolicyReport *report)
{
	ConfigBootstrapContext context = { 0 };
	ClusterControlRootResult result;
	const char *paths[] = { shared_root, wal_root, undo_root };
	bool alias;

	if (report == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	alias = bootstrap_report_overlap(bytes, len, report)
			|| bootstrap_report_overlap(ref, sizeof(*ref), report);
	for (size_t i = 0; i < lengthof(paths); i++)
		alias |= bootstrap_report_overlap(paths[i], paths[i] ? strnlen(paths[i], MAXPGPATH) + 1 : 0,
										  report);
	policy_clear(report);
	if (alias || ref == NULL || bytes == NULL || len == 0 || len > CLUSTER_SHARED_CONFIG_MAX_BYTES)
		return policy_refuse(report, NULL, CLUSTER_CONFIG_POLICY_FORMAT);
	if (node_id < 0 || node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| !(ref->identity.configured[node_id / 64] & (UINT64CONST(1) << (node_id % 64))))
		return policy_refuse(report, NULL, CLUSTER_CONFIG_POLICY_SCOPE);
	for (size_t i = 0; i < lengthof(paths); i++) {
		if (!bootstrap_config_path(paths[i]))
			return policy_refuse(report, NULL, CLUSTER_CONFIG_POLICY_REFERENCE);
		context.paths[i] = paths[i];
	}
	context.policy.report = report;
	context.policy.ref = ref;
	context.policy.native_check = false;
	result = cluster_shared_config_visit(bytes, len, ref, bootstrap_config_visit, &context);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (report->reason == CLUSTER_CONFIG_POLICY_OK)
			report->reason = CLUSTER_CONFIG_POLICY_FORMAT;
		return result;
	}
	for (size_t i = 0; i < lengthof(bootstrap_required); i++) {
		if (!context.seen[i]) {
			ClusterSharedConfigEntry missing
				= { CLUSTER_SHARED_CONFIG_COMMON, bootstrap_required[i].name, NULL };
			return policy_refuse(report, &missing, CLUSTER_CONFIG_POLICY_MISSING);
		}
	}
	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		uint64 bit = UINT64CONST(1) << (node % 64);
		if ((ref->identity.configured[node / 64] & bit) && !(context.nodes[node / 64] & bit)) {
			ClusterSharedConfigEntry missing = { node, "cluster.node_id", NULL };
			return policy_refuse(report, &missing, CLUSTER_CONFIG_POLICY_MISSING);
		}
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

typedef struct ConfigApplyContext {
	int node_id;
	uint32 applied;
	bool apply;
	bool wal_buffers_only;
} ConfigApplyContext;

/* Native assignment hooks need not be reversible. A partial startup may exit,
 * but must not return a usable configuration or a receipt to an admission path.
 * No values are copied into these diagnostics.
 */
static void
startup_config_refuse(const char *message, const ClusterSharedConfigPolicyReport *report)
{
	ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg_internal("%s", message),
					errdetail("Configuration reason=%d node=%d parameter=%s.", report->reason,
							  report->node_id, report->name)));
}

static ClusterControlRootResult
startup_config_visit(const ClusterSharedConfigEntry *entry, void *arg)
{
	ConfigApplyContext *context = arg;
	struct config_generic *record;
	ClusterSharedConfigPolicyReport report;
	if (entry->node_id != CLUSTER_SHARED_CONFIG_COMMON && entry->node_id != context->node_id)
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	/* PGRAC: COMMON precedes INSTANCE in the canonical object. Native -1
	 * conversion must use final instance shared_buffers and selected geometry.
	 * Keep the existing hook, but run this one dependent setting last. */
	if (context->apply && (strcmp(entry->name, "wal_buffers") == 0) != context->wal_buffers_only)
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	policy_clear(&report);
	record = find_option(entry->name, false, true, DEBUG1);
	if (record == NULL) {
		policy_refuse(&report, entry, CLUSTER_CONFIG_POLICY_UNKNOWN);
		startup_config_refuse("shared configuration assignment failed", &report);
	}
	if (!context->apply) {
		if (record->source > PGC_S_FILE || record->reset_source > PGC_S_FILE) {
			policy_refuse(&report, entry, CLUSTER_CONFIG_POLICY_CONTEXT);
			startup_config_refuse("shared configuration conflicts with a higher-priority source",
								  &report);
		}
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	if (set_config_option_ext(entry->name, entry->value, PGC_POSTMASTER, PGC_S_FILE,
							  BOOTSTRAP_SUPERUSERID, GUC_ACTION_SET, true, DEBUG1, false)
			!= 1
		|| record->source != PGC_S_FILE || record->reset_source != PGC_S_FILE
		|| (record->status & GUC_PENDING_RESTART)) {
		policy_refuse(&report, entry, CLUSTER_CONFIG_POLICY_VALUE);
		startup_config_refuse("shared configuration assignment failed", &report);
	}
	++context->applied;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static void
startup_config_release(ResourceOwner owner, ResourceOwner saved_owner, bool success)
{
	bool top_level = saved_owner == NULL;
	/* A parentless startup owner is not a subtransaction and is not the
	 * TopTransactionResourceOwner. There are no database locks to transfer.
	 */
	ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, success, top_level);
	ResourceOwnerRelease(owner, RESOURCE_RELEASE_LOCKS, success, top_level);
	ResourceOwnerRelease(owner, RESOURCE_RELEASE_AFTER_LOCKS, success, top_level);
	CurrentResourceOwner = saved_owner;
	ResourceOwnerDelete(owner);
}

void
cluster_shared_config_apply_startup(const char *bytes, size_t len,
									const ClusterSharedConfigRef *ref, int node_id,
									ClusterSharedConfigApplied *out)
{
	ClusterSharedConfigPolicyReport report;
	ConfigPolicyContext policy = { &report, ref, false };
	ConfigApplyContext context = { node_id, 0, false, false };
	ResourceOwner saved_owner = CurrentResourceOwner;
	ResourceOwner owner;
	MemoryContext saved_context = CurrentMemoryContext;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (IsUnderPostmaster || IsBootstrapProcessingMode() || process_shared_preload_libraries_done)
		ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("shared configuration application requires early startup")));
	policy_clear(&report);
	if (out == NULL || ref == NULL)
		startup_config_refuse("shared configuration object is not applicable", &report);
	if (node_id < 0 || node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| !(ref->identity.configured[node_id / 64] & (UINT64CONST(1) << (node_id % 64))))
		startup_config_refuse("shared configuration node is not configured", &report);

	/* OpenSSL-backed PG hashing requires a resource owner even before shmem or
	 * a transaction exists. Own it here, not in a test-only startup environment.
	 */
	owner = ResourceOwnerCreate(saved_owner, "shared configuration startup");
	CurrentResourceOwner = owner;
	PG_TRY();
	{
		if (cluster_shared_config_visit(bytes, len, ref, policy_visit, &policy)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			startup_config_refuse("shared configuration object is not applicable", &report);
		/* Reject priority conflicts for every selected entry before assignments. */
		if (cluster_shared_config_visit(bytes, len, ref, startup_config_visit, &context)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			startup_config_refuse("shared configuration object is not applicable", &report);
		context.apply = true;
		if (cluster_shared_config_visit(bytes, len, ref, startup_config_visit, &context)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			startup_config_refuse("shared configuration assignment failed", &report);
		context.wal_buffers_only = true;
		if (cluster_shared_config_visit(bytes, len, ref, startup_config_visit, &context)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			startup_config_refuse("shared configuration assignment failed", &report);
		/* Recheck native hooks against the applied common context, including
		 * other nodes' entries. A successful local subset is not a valid image.
		 */
		if (cluster_shared_config_check_gucs(bytes, len, ref, &report)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			startup_config_refuse("shared configuration native validation failed", &report);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(saved_context);
		FlushErrorState();
		startup_config_release(owner, saved_owner, false);
		ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("shared configuration startup callback failed")));
	}
	PG_END_TRY();
	startup_config_release(owner, saved_owner, true);
	out->ref = *ref;
	out->node_id = node_id;
	out->applied_entries = context.applied;
}
