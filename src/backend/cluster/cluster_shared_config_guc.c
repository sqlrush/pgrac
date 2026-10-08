/*-------------------------------------------------------------------------
 * PGRAC: native policy for root-selected configuration entries.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <math.h>
#include "access/commit_ts.h"
#include "access/xlog.h"
#include "catalog/pg_authid_d.h"
#include "common/cryptohash.h"
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_cf_enqueue.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/wait_event.h"
#include "cluster_control_root_private.h"

#define POLICY_COMMON 1
#define POLICY_INSTANCE 2
#define POLICY_COLD 4
#define POLICY_STRING 8
#define POLICY_PATH 16
#define POLICY_UUID 32

/* Fork inherits these bytes together with the actual native defaults. No
 * shared target/barrier can overwrite the process's actual old image. */
static ClusterSharedConfigImage config_process_image;
static ClusterSharedConfigProcess config_process_state;
static bool reload_overlap(const void *a, size_t na, const void *b, size_t nb);
static void startup_config_release(ResourceOwner owner, ResourceOwner saved_owner, bool success);

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
	{ "cluster.storage_quorum_cluster", POLICY_COMMON | POLICY_COLD | POLICY_STRING },
	{ "cluster.storage_quorum_nodes", POLICY_COMMON | POLICY_COLD | POLICY_STRING },
	{ "cluster.undo_gcs_coherence", POLICY_COMMON | POLICY_COLD },
	{ "cluster.enabled", POLICY_COMMON | POLICY_COLD },
	{ "debug_io_direct", POLICY_COMMON | POLICY_COLD | POLICY_STRING },
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

/* The parameters pg_control records at creation (XLOG_PARAMETER_CHANGE).
 * A shared cluster cannot change them; StartupXLOG refuses a differing
 * value as the backstop. */
static const char *const config_recorded_parameters[] = { "wal_level",
														  "wal_log_hints",
														  "max_connections",
														  "max_worker_processes",
														  "max_wal_senders",
														  "max_prepared_transactions",
														  "max_locks_per_transaction",
														  "track_commit_timestamp" };

static bool
config_recorded_parameter(const char *name)
{
	for (size_t i = 0; i < lengthof(config_recorded_parameters); ++i)
		if (strcmp(name, config_recorded_parameters[i]) == 0)
			return true;
	return false;
}

static unsigned
config_policy_flags(const char *name)
{
	for (size_t i = 0; i < lengthof(config_policies); ++i)
		if (strcmp(name, config_policies[i].name) == 0)
			return config_policies[i].policy;
	return POLICY_COMMON;
}

/* SQL owns only persistence. The existing background delivery owner still
 * runs if this backend exits immediately after the root becomes durable. */
void
cluster_shared_config_alter_system(const char *name, const char *value)
{
	ClusterSharedConfigEntry change;
	ClusterSharedConfigPublication published;
	ClusterSharedConfigPolicyReport report;
	ClusterControlRootResult result;
	uint64 epoch = cluster_epoch_get_current();
	uint64 incarnation = cluster_qvotec_get_self_incarnation();
	char *canonical;
	if (name == NULL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("shared ALTER SYSTEM RESET ALL is not supported")));
	canonical = pstrdup(name);
	for (char *p = canonical; *p != '\0'; ++p)
		if (*p >= 'A' && *p <= 'Z')
			*p += 'a' - 'A';
	if (config_recorded_parameter(canonical))
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_SHARED_PARAMETER_FIXED),
				 errmsg("parameter \"%s\" cannot be changed in a shared cluster", canonical),
				 errdetail("PGRAC_FAMILY=SHARED_CONFIG PGRAC_REASON=RECORDED_PARAMETER"),
				 errhint("The shared control file records this parameter when the cluster is "
						 "created; keep that value.")));
	/* A nested caller cannot retire somebody else's CF request. In particular
	 * do not turn this precondition into an endless LOCK_UNAVAILABLE retry. */
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("shared configuration publication requires an idle CF owner"),
						errdetail("PGRAC_REASON=CONFIG_PUBLICATION_CF_BUSY")));
	change.name = canonical;
	change.value = value == NULL ? NULL : pg_server_to_any(value, strlen(value), PG_UTF8);
	change.node_id = (config_policy_flags(canonical) & POLICY_INSTANCE)
						 ? cluster_node_id
						 : CLUSTER_SHARED_CONFIG_COMMON;
	for (;;) {
		CHECK_FOR_INTERRUPTS();
		if (cluster_epoch_get_current() != epoch
			|| cluster_qvotec_get_self_incarnation() != incarnation)
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		else
			result = cluster_control_root_config_change(&change, &published, &report);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			break;
		if (result != CLUSTER_CONTROL_ROOT_CAS_CONFLICT
			&& result != CLUSTER_CONTROL_ROOT_ADMISSION_PENDING
			&& result != CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE)
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("shared configuration publication refused"),
							errdetail("PGRAC_REASON=CONFIG_PUBLICATION_REFUSED result=%d target=%d",
									  (int)result, change.node_id)));
		/* No CF is held here. This is retry scheduling, never a success ACK
		 * or a deadline that turns healthy contention into an SQL error. */
		ResetLatch(MyLatch);
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 10L,
						PG_WAIT_EXTENSION);
	}
	if (change.value != value)
		pfree((void *)change.value);
	pfree(canonical);
}

/* This is the common protocol/recovery observation set, not the broader
 * COMMON namespace of session defaults. Unknown common cluster scalars are
 * included conservatively; unsupported/test/instance entries are not. No
 * check or SHOW hook runs here, including from native abort restoration.
 */
static bool
config_common_active_record(const struct config_generic *record)
{
	static const char *const native[] = { "wal_level",
										  "wal_log_hints",
										  "max_connections",
										  "max_worker_processes",
										  "max_wal_senders",
										  "max_prepared_transactions",
										  "max_locks_per_transaction",
										  "track_commit_timestamp" };
	unsigned flags;
	const char *name;
	if (record == NULL || record->name == NULL || record->context == PGC_INTERNAL
		|| (record->flags & (GUC_CUSTOM_PLACEHOLDER | GUC_DISALLOW_IN_FILE)))
		return false;
	name = record->name;
	if (strncmp(name, "cluster.", 8) != 0) {
		for (size_t i = 0; i < lengthof(native); ++i)
			if (strcmp(name, native[i]) == 0)
				return true;
		return false;
	}
	if (strcmp(name, "cluster.injection_points") == 0 || strncmp(name, "cluster.test_", 13) == 0
		|| strncmp(name, "cluster.gcs_block_drop_", 23) == 0
		|| strcmp(name, "cluster.xnode_profile") == 0 || strcmp(name, "cluster.update_trace") == 0)
		return false;
	flags = config_policy_flags(name);
	return (flags & POLICY_COMMON) != 0
		   && (record->vartype != PGC_STRING || (flags & POLICY_STRING) != 0);
}

static bool
config_common_use_record(const struct config_generic *record)
{
	return config_common_active_record(record)
		   && (record->context == PGC_POSTMASTER || record->context == PGC_SIGHUP);
}

bool
cluster_shared_config_restart_only(const struct config_generic *record)
{
	/* Shared FILE ownership also covers the real process-local applier before
	 * full native startup admission. Ordinary non-shared PG is unchanged. */
	return record != NULL && (cluster_shared_config || (record->status & GUC_SHARED_FILE))
		   && config_common_use_record(record);
}


static bool
config_active_u32(pg_cryptohash_ctx *hash, uint32 value)
{
	uint8 bytes[4];
	for (int i = 0; i < 4; ++i)
		bytes[i] = (uint8)(value >> (8 * i));
	return pg_cryptohash_update(hash, bytes, sizeof(bytes)) == 0;
}

static bool
config_active_value(pg_cryptohash_ctx *hash, const struct config_generic *record)
{
	uint8 scalar[8];
	uint64 bits = 0;
	const uint8 *bytes = scalar;
	size_t len = sizeof(scalar);
	bool is_null = false;
	switch (record->vartype) {
	case PGC_BOOL:
		bits = *((const struct config_bool *)record)->variable ? 1 : 0;
		break;
	case PGC_INT:
		bits = (uint64)(int64) * ((const struct config_int *)record)->variable;
		break;
	case PGC_ENUM:
		bits = (uint64)(int64) * ((const struct config_enum *)record)->variable;
		break;
	case PGC_REAL: {
		double value = *((const struct config_real *)record)->variable;
		if (!isfinite(value) || sizeof(value) != sizeof(bits))
			return false;
		/* Normalize signed zero; all other finite values have one native
			 * binary64 representation on the homogeneous supported profile. */
		if (value != 0)
			memcpy(&bits, &value, sizeof(bits));
		break;
	}
	case PGC_STRING:
		bytes = (const uint8 *)*((const struct config_string *)record)->variable;
		is_null = bytes == NULL;
		len = is_null ? 0 : strnlen((const char *)bytes, CLUSTER_SHARED_CONFIG_MAX_VALUE + 1);
		if (len > CLUSTER_SHARED_CONFIG_MAX_VALUE)
			return false;
		break;
	default:
		return false;
	}
	if (record->vartype != PGC_STRING)
		for (int i = 0; i < 8; ++i)
			scalar[i] = (uint8)(bits >> (8 * i));
	return config_active_u32(hash, (uint32)strlen(record->name))
		   && pg_cryptohash_update(hash, (const uint8 *)record->name, strlen(record->name)) == 0
		   && config_active_u32(hash, (uint32)record->vartype)
		   && config_active_u32(hash, is_null ? 0 : 1) && config_active_u32(hash, (uint32)len)
		   && (len == 0 || pg_cryptohash_update(hash, bytes, len) == 0);
}

static bool
config_active_build(ClusterSharedConfigActive *out)
{
	static const char *const domains[] = { "PGRAC-use-static-v2", "PGRAC-use-dynamic-v2" };
	pg_cryptohash_ctx *hash[2] = { NULL, NULL };
	struct config_generic **records;
	uint32 counts[2] = { 0, 0 };
	int count;
	bool valid = false;
	uint8 digests[2][32];
	records = get_guc_variables(&count);
	if (count <= 0 || count > CLUSTER_SHARED_CONFIG_MAX_ENTRIES)
		goto done;
	for (int i = 0; i < 2; ++i) {
		hash[i] = pg_cryptohash_create(PG_SHA256);
		if (hash[i] == NULL || pg_cryptohash_init(hash[i]) != 0
			|| pg_cryptohash_update(hash[i], (const uint8 *)domains[i], strlen(domains[i])) != 0)
			goto done;
	}
	for (int i = 0; i < count; ++i) {
		struct config_generic *record = records[i];
		int part;
		if (!config_common_use_record(record))
			continue;
		part = record->context == PGC_POSTMASTER ? 0 : 1;
		if (strlen(record->name) > CLUSTER_SHARED_CONFIG_MAX_NAME
			|| !config_active_value(hash[part], record))
			goto done;
		counts[part]++;
	}
	for (int i = 0; i < 2; ++i)
		if (counts[i] == 0 || !config_active_u32(hash[i], counts[i])
			|| pg_cryptohash_final(hash[i], digests[i], sizeof(digests[i])) != 0)
			goto done;
	out->version = CLUSTER_SHARED_CONFIG_COMMON_VERSION;
	out->static_entries = counts[0];
	out->dynamic_entries = counts[1];
	memcpy(out->static_sha256, digests[0], 32);
	memcpy(out->dynamic_sha256, digests[1], 32);
	valid = true;
done:
	for (int i = 0; i < 2; ++i)
		if (hash[i] != NULL)
			pg_cryptohash_free(hash[i]);
	pfree(records);
	return valid;
}

static bool
config_native_profile(ClusterSharedConfigActive *out)
{
	ClusterSharedConfigActive result = { 0 };
	ResourceOwner saved_owner = CurrentResourceOwner, owner;
	MemoryContext saved_context = CurrentMemoryContext, scratch;
	bool valid;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (config_process_image.bytes == NULL || config_process_state.applier_pid <= 0
		|| config_process_state.failed || config_process_state.parallel_snapshot)
		return false;
	/* Real PM/logger have no transaction resource owner. Own crypto/scratch
	 * here; never use a test-only setup to make their native path succeed. */
	scratch = AllocSetContextCreate(saved_context, "common native profile", ALLOCSET_SMALL_SIZES);
	MemoryContextSwitchTo(scratch);
	owner = ResourceOwnerCreate(saved_owner, "common native profile");
	CurrentResourceOwner = owner;
	PG_TRY();
	{
		valid = config_active_build(&result);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(saved_context);
		startup_config_release(owner, saved_owner, false);
		MemoryContextDelete(scratch);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(saved_context);
	startup_config_release(owner, saved_owner, true);
	MemoryContextDelete(scratch);
	if (valid)
		*out = result;
	return valid;
}


bool
cluster_shared_config_common_profile(ClusterSharedConfigActive *out)
{
	return config_native_profile(out);
}

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
	policy = config_policy_flags(entry->name);
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

	/* PGRAC: the shared recovery profile does not implement this optional
	 * native SIDE. Refuse persistence too, not just its later activation. */
	if (strcmp(entry->name, "track_commit_timestamp") == 0) {
		bool enabled;
		if (!parse_bool(entry->value, &enabled))
			return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_VALUE);
		if (enabled)
			return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_UNSUPPORTED);
	}
	if (strcmp(entry->name, "wal_level") == 0 && pg_strcasecmp(entry->value, "logical") == 0)
		return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_UNSUPPORTED);

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
	if (record->context == PGC_POSTMASTER || config_common_use_record(record))
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

/* Observation only: failed/pending state and an inherited PID never grant
 * admission. Ordinary parameter delivery follows native SIGHUP semantics.
 * Author: SqlRush <sqlrush@gmail.com>
 */
bool
cluster_shared_config_process_observe(ClusterSharedConfigProcess *out)
{
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!config_process_state.parallel_snapshot
		&& (config_process_image.bytes == NULL || config_process_state.applier_pid <= 0))
		return false;
	*out = config_process_state;
	return true;
}

bool
cluster_shared_config_process_copy(ClusterSharedConfigProcess *out, ClusterSharedConfigImage *image)
{
	ClusterSharedConfigImage copy = { 0 };
	if (reload_overlap(out, sizeof(*out), image, sizeof(*image)))
		return false;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (image != NULL)
		memset(image, 0, sizeof(*image));
	if (out == NULL || image == NULL || config_process_image.bytes == NULL
		|| config_process_state.failed || config_process_state.parallel_snapshot
		|| config_process_state.applier_pid <= 0)
		return false;
	copy.bytes = palloc(config_process_image.len + 1);
	copy.len = config_process_image.len;
	memcpy(copy.bytes, config_process_image.bytes, copy.len + 1);
	*out = config_process_state;
	*image = copy;
	return true;
}

/* The parallel leader's active GUCs are not the image inherited from the PM.
 * Discard the copied image BEFORE native reset/assign hooks can run or fail.
 * Query semantics remain native; this worker does not certify application of
 * the selected root. Static common values remain fixed until full restart.
 * Author: SqlRush <sqlrush@gmail.com>
 */
void
cluster_shared_config_process_parallel_restore(void)
{
	uint32 node_id = config_process_state.node_id;
	bool failed = config_process_state.failed;
	if (config_process_image.bytes == NULL)
		return;
	cluster_shared_config_free(&config_process_image);
	memset(&config_process_state, 0, sizeof(config_process_state));
	config_process_state.node_id = node_id;
	config_process_state.failed = failed;
	config_process_state.parallel_snapshot = true;
}

ClusterControlRootResult
cluster_shared_config_process_reload(const char *bytes, size_t len,
									 const ClusterSharedConfigRef *ref,
									 ClusterSharedConfigProcess *out,
									 ClusterSharedConfigPolicyReport *report)
{
	ClusterSharedConfigIdentity identity;
	ClusterSharedConfigReload applied;
	ClusterControlRootResult result;
	char *replacement;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (report != NULL)
		policy_clear(report);
	if (out == NULL || report == NULL || ref == NULL || bytes == NULL || len == 0
		|| len > CLUSTER_SHARED_CONFIG_MAX_BYTES || config_process_image.bytes == NULL
		|| config_process_state.failed || config_process_state.applier_pid <= 0)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	identity = ref->identity;
	identity.generation = config_process_state.ref.identity.generation;
	if (memcmp(&identity, &config_process_state.ref.identity, sizeof(identity)) != 0
		|| ref->identity.generation < identity.generation)
		return policy_refuse(report, NULL, CLUSTER_CONFIG_POLICY_REFERENCE);
	if (ref->identity.generation == identity.generation) {
		if (memcmp(ref, &config_process_state.ref, sizeof(*ref)) != 0
			|| len != config_process_image.len || memcmp(bytes, config_process_image.bytes, len))
			return policy_refuse(report, NULL, CLUSTER_CONFIG_POLICY_REFERENCE);
		/* No hooks, source changes or false child-only deferred obligation when
		 * this exact default was already inherited/applied in this process. */
		*out = config_process_state;
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}

	/* Own the next image before any assign hook. A caller's transaction context
	 * or cancellation must not free the old/new process-lifetime defaults. */
	replacement = MemoryContextAlloc(TopMemoryContext, len + 1);
	memcpy(replacement, bytes, len);
	replacement[len] = '\0';
	PG_TRY();
	{
		result = cluster_shared_config_apply_reload(
			config_process_image.bytes, config_process_image.len, &config_process_state.ref,
			replacement, len, ref, config_process_state.node_id, &applied, report);
	}
	PG_CATCH();
	{
		/* Native hook side effects are not transactional. Keep the last
		 * successful ref for diagnosis, but never issue another success from
		 * this potentially partially modified process. The control owner must
		 * retain its retirement/repair lane while dependent data is held. */
		config_process_state.failed = true;
		pfree(replacement);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		config_process_state.failed = true;
		pfree(replacement);
		return result;
	}
	pfree(config_process_image.bytes);
	config_process_image.bytes = replacement;
	config_process_image.len = len;
	config_process_state.ref = applied.ref;
	config_process_state.pending_restart_total = applied.pending_restart_total;
	config_process_state.deferred_total = applied.deferred_total;
	config_process_state.applier_pid = MyProcPid;
	*out = config_process_state;
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

/* PGRAC: changed-entry production is qualified before publication wiring.
 * Author: SqlRush <sqlrush@gmail.com> */
ClusterControlRootResult
cluster_shared_config_prepare_change(const char *shared_root, const char *bytes, size_t len,
									 const ClusterSharedConfigRef *ref,
									 const ClusterSharedConfigEntry *change,
									 const uint8 operation_uuid[16], ClusterSharedConfigStage *out,
									 bool *changed, ClusterSharedConfigPolicyReport *report)
{
	ClusterSharedConfigImage amended;
	ClusterSharedConfigRef next;
	ClusterControlRootResult result;
	ClusterSharedConfigEntry checked;
	bool modifies;
	char old_value[CLUSTER_SHARED_CONFIG_MAX_VALUE + 1];

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (changed != NULL)
		*changed = false;
	if (report != NULL)
		policy_clear(report);
	if (out == NULL || changed == NULL || report == NULL || change == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	/* A request cannot repair an invalid selected object or silently drop an
	 * invalid untouched remote-node entry. All checks precede staging I/O. */
	result = cluster_shared_config_check_gucs(bytes, len, ref, report);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_shared_config_amend(bytes, len, ref, change, &amended, &next, &modifies);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		report->reason = CLUSTER_CONFIG_POLICY_FORMAT;
		return result;
	}
	if (!modifies) {
		cluster_shared_config_free(&amended);
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	checked = *change;
	/* Policy needs a literal even for deletion. Checking the old value with
	 * online_change=true prevents RESET from erasing a cold identity/layout
	 * entry which SET would correctly refuse. Absence was a no-op above. */
	if (checked.value == NULL) {
		result = cluster_shared_config_lookup(bytes, len, ref, checked.node_id, checked.name,
											  old_value, sizeof(old_value));
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			cluster_shared_config_free(&amended);
			report->reason = CLUSTER_CONFIG_POLICY_FORMAT;
			return result;
		}
		checked.value = old_value;
	}
	PG_TRY();
	{
		result = cluster_shared_config_check_entry(&checked, true, report);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = cluster_shared_config_prepare_gucs(shared_root, amended.bytes, amended.len,
														&next, operation_uuid, out, report);
	}
	PG_CATCH();
	{
		cluster_shared_config_free(&amended);
		PG_RE_THROW();
	}
	PG_END_TRY();
	cluster_shared_config_free(&amended);
	*changed = result == CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	return result;
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
	record->status |= GUC_SHARED_FILE;
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
	char *remembered = NULL;

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
		remembered = MemoryContextAlloc(TopMemoryContext, len + 1);
		memcpy(remembered, bytes, len);
		remembered[len] = '\0';
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
	/* Check actual inherited/command-line values even if the selected object
	 * omitted the parameter. Never publish a successful shared application. */
	if (track_commit_timestamp)
		ereport(FATAL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("shared configuration requires track_commit_timestamp=off"),
				 errdetail("PGRAC_FAMILY=SHARED_CONFIG PGRAC_REASON=COMMIT_TS_UNSUPPORTED")));
	if (wal_level == WAL_LEVEL_LOGICAL)
		ereport(FATAL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("shared configuration does not support wal_level=logical"),
				 errdetail("PGRAC_FAMILY=SHARED_CONFIG PGRAC_REASON=LOGICAL_WAL_UNSUPPORTED")));
	if (config_process_image.bytes != NULL)
		pfree(config_process_image.bytes);
	config_process_image.bytes = remembered;
	config_process_image.len = len;
	memset(&config_process_state, 0, sizeof(config_process_state));
	config_process_state.ref = *ref;
	config_process_state.node_id = node_id;
	config_process_state.applier_pid = MyProcPid;
	out->ref = *ref;
	out->node_id = node_id;
	out->applied_entries = context.applied;
}

/* PGRAC: native online application only. LMON separately selects/revalidates
 * the root and notifies the parent; native reload has no application census.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct ConfigReloadItem {
	ClusterSharedConfigEntry entry;
	bool removed;
	bool changed;
} ConfigReloadItem;

typedef struct ConfigReloadList {
	ConfigReloadItem *items;
	uint32 count;
	ConfigPolicyContext policy;
} ConfigReloadList;

static ClusterControlRootResult
reload_collect(const ClusterSharedConfigEntry *entry, void *arg)
{
	ConfigReloadList *list = arg;
	ClusterControlRootResult result = policy_visit(entry, &list->policy);
	ConfigReloadItem *item;
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (list->count >= CLUSTER_SHARED_CONFIG_MAX_ENTRIES)
		return policy_refuse(list->policy.report, entry, CLUSTER_CONFIG_POLICY_FORMAT);
	item = &list->items[list->count++];
	item->entry.node_id = entry->node_id;
	item->entry.name = pstrdup(entry->name);
	item->entry.value = pstrdup(entry->value);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static int
reload_key_compare(const ClusterSharedConfigEntry *a, const ClusterSharedConfigEntry *b)
{
	if (a->node_id != b->node_id)
		return a->node_id < b->node_id ? -1 : 1;
	return strcmp(a->name, b->name);
}

static bool
reload_selected(const ClusterSharedConfigEntry *entry, int node_id)
{
	return entry->node_id == CLUSTER_SHARED_CONFIG_COMMON || entry->node_id == node_id;
}

static ClusterControlRootResult
reload_diff(ConfigReloadList *old, ConfigReloadList *next, ClusterSharedConfigPolicyReport *report)
{
	uint32 a = 0, b = 0;
	/* Canonical keys are ordered common, then fixed-width instance id, name.
	 * One merge pass validates ALL changes/deletions before any assignment;
	 * do not hash and search the whole MiB-sized image once for every key. */
	while (a < old->count || b < next->count) {
		ConfigReloadItem *x = a < old->count ? &old->items[a] : NULL;
		ConfigReloadItem *y = b < next->count ? &next->items[b] : NULL;
		const ClusterSharedConfigEntry *changed = NULL;
		int order = x == NULL ? 1 : y == NULL ? -1 : reload_key_compare(&x->entry, &y->entry);
		if (order < 0) {
			x->removed = true;
			changed = &x->entry;
			++a;
		} else if (order > 0) {
			changed = &y->entry;
			y->changed = true;
			++b;
		} else {
			if (strcmp(x->entry.value, y->entry.value) != 0) {
				changed = &y->entry;
				y->changed = true;
			}
			++a;
			++b;
		}
		if (changed != NULL) {
			ClusterControlRootResult result = policy_check(changed, true, true, report);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				return result;
		}
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
reload_collect_pair(const char *old_bytes, size_t old_len, const ClusterSharedConfigRef *old_ref,
					const char *new_bytes, size_t new_len, const ClusterSharedConfigRef *new_ref,
					ConfigReloadList *old, ConfigReloadList *next,
					ClusterSharedConfigPolicyReport *report)
{
	ClusterControlRootResult result;
	*old
		= (ConfigReloadList){ palloc0(sizeof(ConfigReloadItem) * CLUSTER_SHARED_CONFIG_MAX_ENTRIES),
							  0,
							  { report, old_ref, true } };
	*next
		= (ConfigReloadList){ palloc0(sizeof(ConfigReloadItem) * CLUSTER_SHARED_CONFIG_MAX_ENTRIES),
							  0,
							  { report, new_ref, true } };
	result = cluster_shared_config_visit(old_bytes, old_len, old_ref, reload_collect, old);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_shared_config_visit(new_bytes, new_len, new_ref, reload_collect, next);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY && report->reason == CLUSTER_CONFIG_POLICY_OK)
		report->reason = CLUSTER_CONFIG_POLICY_FORMAT;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = reload_diff(old, next, report);
	return result;
}

static ClusterControlRootResult
reload_assign(ConfigReloadList *old, ConfigReloadList *next, int node_id,
			  ClusterSharedConfigReload *applied, ClusterSharedConfigPolicyReport *report)
{
	for (uint32 i = 0; i < old->count; ++i) {
		ConfigReloadItem *item = &old->items[i];
		struct config_generic *record;
		if (!item->removed || !reload_selected(&item->entry, node_id))
			continue;
		record = find_option(item->entry.name, false, true, DEBUG1);
		if (record == NULL)
			return policy_refuse(report, &item->entry, CLUSTER_CONFIG_POLICY_UNKNOWN);
		record->status |= GUC_SHARED_FILE;
		switch (ClusterResetConfigFileSetting(item->entry.name)) {
		case GUC_FILE_RESET_DONE:
			record->status &= ~GUC_SHARED_DEFERRED;
			++applied->removed_entries;
			break;
		case GUC_FILE_RESET_PENDING_RESTART:
			++applied->pending_restart_entries;
			break;
		case GUC_FILE_RESET_DEFERRED:
			record->status |= GUC_SHARED_DEFERRED;
			++applied->deferred_entries;
			break;
		default:
			return policy_refuse(report, &item->entry, CLUSTER_CONFIG_POLICY_VALUE);
		}
	}
	/* Same ordering as ProcessConfigFile: remove FILE values, restore native
	 * environment/dynamic defaults, then overlay the selected FILE entries. */
	ClusterRestoreConfigFileDefaults();
	for (uint32 i = 0; i < next->count; ++i) {
		ClusterSharedConfigEntry *entry = &next->items[i].entry;
		struct config_generic *record;
		int result;
		if (!reload_selected(entry, node_id))
			continue;
		record = find_option(entry->name, false, true, DEBUG1);
		if (record == NULL)
			return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_UNKNOWN);
		record->status |= GUC_SHARED_FILE;
		result = set_config_option_ext(entry->name, entry->value, PGC_SIGHUP, PGC_S_FILE,
									   BOOTSTRAP_SUPERUSERID, GUC_ACTION_SET, true, DEBUG1, false);
		if ((record->context == PGC_POSTMASTER || cluster_shared_config_restart_only(record))
			&& (record->status & GUC_PENDING_RESTART)) {
			++applied->pending_restart_entries;
			continue;
		}
		if (result == 0)
			return policy_refuse(report, entry, CLUSTER_CONFIG_POLICY_VALUE);
		if (IsUnderPostmaster
			&& (record->context == PGC_BACKEND || record->context == PGC_SU_BACKEND)) {
			/* Native SIGHUP ignores even unchanged BACKEND entries. Do not
			 * manufacture deferred debt for defaults actually inherited from
			 * the parent. Existing debt survives an unrelated generation. */
			if (next->items[i].changed)
				record->status |= GUC_SHARED_DEFERRED;
			if (record->status & GUC_SHARED_DEFERRED)
				++applied->deferred_entries;
			else
				++applied->applied_entries;
		} else {
			record->status &= ~GUC_SHARED_DEFERRED;
			++applied->applied_entries;
		}
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static bool
reload_overlap(const void *a, size_t na, const void *b, size_t nb)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	if (a == NULL || b == NULL || na == 0 || nb == 0)
		return false;
	return x <= y ? y - x < na : x - y < nb;
}

ClusterControlRootResult
cluster_shared_config_apply_reload(const char *old_bytes, size_t old_len,
								   const ClusterSharedConfigRef *old_ref, const char *new_bytes,
								   size_t new_len, const ClusterSharedConfigRef *new_ref,
								   int node_id, ClusterSharedConfigReload *out,
								   ClusterSharedConfigPolicyReport *report)
{
	ClusterSharedConfigIdentity same;
	ClusterControlRootResult result;
	ClusterSharedConfigReload applied = { 0 };
	ConfigReloadList old, next;
	MemoryContext saved_context = CurrentMemoryContext, context;
	ResourceOwner saved_owner = CurrentResourceOwner, owner;
	const void *inputs[] = { old_bytes, old_ref, new_bytes, new_ref };
	size_t sizes[] = { old_len, sizeof(*old_ref), new_len, sizeof(*new_ref) };

	/* Reject carrier aliasing BEFORE clearing the caller's old reference. */
	if (reload_overlap(out, sizeof(*out), report, sizeof(*report)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	for (size_t i = 0; i < lengthof(inputs); ++i)
		if (reload_overlap(inputs[i], sizes[i], out, sizeof(*out))
			|| reload_overlap(inputs[i], sizes[i], report, sizeof(*report)))
			return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (report == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	policy_clear(report);
	if (out == NULL || old_ref == NULL || new_ref == NULL || node_id < 0
		|| node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| !(new_ref->identity.configured[node_id / 64] & (UINT64CONST(1) << (node_id % 64))))
		return policy_refuse(report, NULL, CLUSTER_CONFIG_POLICY_FORMAT);
	same = old_ref->identity;
	same.generation = new_ref->identity.generation;
	if (memcmp(&same, &new_ref->identity, sizeof(same)) != 0
		|| new_ref->identity.generation < old_ref->identity.generation
		|| (new_ref->identity.generation == old_ref->identity.generation
			&& memcmp(old_ref->sha256, new_ref->sha256, 32) != 0))
		return policy_refuse(report, NULL, CLUSTER_CONFIG_POLICY_FORMAT);

	context = AllocSetContextCreate(saved_context, "shared configuration reload",
									ALLOCSET_DEFAULT_SIZES);
	owner = ResourceOwnerCreate(saved_owner, "shared configuration reload");
	CurrentResourceOwner = owner;
	MemoryContextSwitchTo(context);
	PG_TRY();
	{
		result = reload_collect_pair(old_bytes, old_len, old_ref, new_bytes, new_len, new_ref, &old,
									 &next, report);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = reload_assign(&old, &next, node_id, &applied, report);
		/* Native hooks may depend on other settings. Validate again against the
		 * actual new context; neither a partial apply nor pending restart proves
		 * a simultaneously usable deployment profile. */
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = cluster_shared_config_check_gucs(new_bytes, new_len, new_ref, report);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(saved_context);
		startup_config_release(owner, saved_owner, false);
		MemoryContextDelete(context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(saved_context);
	startup_config_release(owner, saved_owner, result == CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	MemoryContextDelete(context);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		ClusterConfigFilePending(&applied.pending_restart_total, &applied.deferred_total);
		applied.old_ref = *old_ref;
		applied.ref = *new_ref;
		applied.node_id = node_id;
		*out = applied;
	}
	return result;
}
