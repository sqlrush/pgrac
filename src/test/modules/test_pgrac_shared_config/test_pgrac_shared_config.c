/* PGRAC: test-only native GUC engine integration, no product catalog entry.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_shared_config.h"
#include "common/cryptohash.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/resowner.h"
#endif

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(test_pgrac_config_entry);
PG_FUNCTION_INFO_V1(test_pgrac_config_object);
PG_FUNCTION_INFO_V1(test_pgrac_config_registration);
PG_FUNCTION_INFO_V1(test_pgrac_config_backend_apply);
PG_FUNCTION_INFO_V1(test_pgrac_config_bootstrap);
PG_FUNCTION_INFO_V1(test_pgrac_control_image);
PG_FUNCTION_INFO_V1(test_pgrac_recovery_capacity);
PGDLLEXPORT void _PG_init(void);

Datum
test_pgrac_recovery_capacity(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test recovery sizing inspection requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterControlRecoveryCapacity required = { 0 };
		ClusterSharedConfigPolicyReport report;
		ClusterControlRootResult result;
		char *field = text_to_cstring(PG_GETARG_TEXT_PP(0));
		uint32 value = (uint32)PG_GETARG_INT64(1);
		const char *names[] = { "max_connections", "max_worker_processes", "max_wal_senders",
								"max_prepared_transactions", "max_locks_per_transaction" };
		uint32 *fields[] = { &required.max_connections, &required.max_worker_processes,
							 &required.max_wal_senders, &required.max_prepared_xacts,
							 &required.max_locks_per_xact };
		bool found = false;
		required.current_sources = 2;
		required.history_sources = 3;
		for (size_t i = 0; i < lengthof(names); i++) {
			*fields[i] = (uint32)strtoul(GetConfigOption(names[i], false, false), NULL, 10);
			if (strcmp(field, names[i]) == 0) {
				*fields[i] = value;
				found = true;
			}
		}
		if (strcmp(field, "current") == 0)
			required.current_sources = value;
		else if (strcmp(field, "history") == 0)
			required.history_sources = value;
		else if (!found && strcmp(field, "valid") && strcmp(field, "null")
				 && strcmp(field, "report-null") && strcmp(field, "alias"))
			ereport(ERROR, (errmsg("unknown test recovery capacity mutation")));
		memset(&report, 0xa5, sizeof(report));
		result = cluster_shared_config_check_recovery_capacity(
			strcmp(field, "null") == 0	  ? NULL
			: strcmp(field, "alias") == 0 ? (ClusterControlRecoveryCapacity *)&report
										  : &required,
			strcmp(field, "report-null") == 0 ? NULL : &report);
		if (strcmp(field, "report-null") == 0)
			PG_RETURN_TEXT_P(cstring_to_text(result != 0 ? "refused" : "unexpected"));
		PG_RETURN_TEXT_P(cstring_to_text(psprintf("%d:%u:%u:%s", result == 0, report.reason,
												  report.checked_entries, report.name)));
	}
#else
	ereport(ERROR, (errmsg("PGRAC cluster build required")));
	PG_RETURN_NULL();
#endif
}

/* Only a fixture builder: no global control/GUC state is replaced. */
Datum
test_pgrac_control_image(PG_FUNCTION_ARGS)
{
	ControlFileData control, before;
	char *field = text_to_cstring(PG_GETARG_TEXT_PP(0));
	FILE *file;
	uint64 sysid = GetSystemIdentifier();
	int old_segment_size = wal_segment_size;
	char *old_size = pstrdup(GetConfigOption("wal_segment_size", false, false));
	char *old_checksums = pstrdup(GetConfigOption("data_checksums", false, false));
	pg_crc32c crc;

	if (!superuser())
		ereport(ERROR, (errmsg("test control inspection requires superuser")));
	file = AllocateFile(XLOG_CONTROL_FILE, "rb");
	if (file == NULL)
		ereport(ERROR, (errmsg("could not open test control image")));
	if (fread(&control, 1, sizeof(control), file) != sizeof(control))
		ereport(ERROR, (errmsg("could not read test control image")));
	if (FreeFile(file) != 0)
		ereport(ERROR, (errmsg("could not close test control image")));

	if (strcmp(field, "version") == 0)
		control.pg_control_version = 1;
	else if (strcmp(field, "endian") == 0)
		control.pg_control_version = 65536;
	else if (strcmp(field, "catalog") == 0)
		control.catalog_version_no = 1;
	else if (strcmp(field, "align") == 0)
		control.maxAlign++;
	else if (strcmp(field, "float") == 0)
		control.floatFormat = 1.0;
	else if (strcmp(field, "block") == 0)
		control.blcksz++;
	else if (strcmp(field, "relseg") == 0)
		control.relseg_size++;
	else if (strcmp(field, "walblock") == 0)
		control.xlog_blcksz++;
	else if (strcmp(field, "name") == 0)
		control.nameDataLen++;
	else if (strcmp(field, "keys") == 0)
		control.indexMaxKeys++;
	else if (strcmp(field, "toast") == 0)
		control.toast_max_chunk_size++;
	else if (strcmp(field, "lob") == 0)
		control.loblksize++;
	else if (strcmp(field, "float8") == 0)
		control.float8ByVal = !control.float8ByVal;
	else if (strcmp(field, "walsize") == 0)
		control.xlog_seg_size = (uint32)PG_GETARG_INT64(1);
	else if (strcmp(field, "valid") != 0 && strcmp(field, "crc") != 0 && strcmp(field, "null") != 0)
		ereport(ERROR, (errmsg("unknown test control mutation")));

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *)&control, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	control.crc = crc;
	/* Wrong version must take precedence even when CRC is also wrong. */
	if (strcmp(field, "crc") == 0 || strcmp(field, "version") == 0 || strcmp(field, "endian") == 0)
		control.crc ^= 1;
	memcpy(&before, &control, sizeof(control));
	XLogValidateControlFile(strcmp(field, "null") == 0 ? NULL : &control);
	PG_RETURN_BOOL(memcmp(&before, &control, sizeof(control)) == 0 && sysid == GetSystemIdentifier()
				   && old_segment_size == wal_segment_size
				   && strcmp(old_size, GetConfigOption("wal_segment_size", false, false)) == 0
				   && strcmp(old_checksums, GetConfigOption("data_checksums", false, false)) == 0);
}

#ifdef USE_PGRAC_CLUSTER
static int test_apply_node = -1;
static bool test_bad_hash;
static bool test_stop_after_apply;

/* The test creates exact bytes, but production code performs all validation
 * and application. No fake native process identity or mutable engine flags.
 */
static char *
application_fixture(const char *body, ClusterSharedConfigRef *ref, size_t *len)
{
	char *bytes = palloc(CLUSTER_SHARED_CONFIG_MAX_BYTES);
	pg_cryptohash_ctx *ctx;
	memset(ref, 0, sizeof(*ref));
	ref->identity.system_identifier = 1234;
	ref->identity.database_incarnation = 1;
	ref->identity.generation = 1;
	ref->identity.configured[0] = 3;
	memset(ref->identity.storage_uuid, 1, 16);
	memset(ref->identity.authority_uuid, 2, 16);
	if (cluster_shared_config_encode(&ref->identity, NULL, 0, bytes,
									 CLUSTER_SHARED_CONFIG_MAX_BYTES, len, ref->sha256)
			!= 0
		|| strlen(body) >= CLUSTER_SHARED_CONFIG_MAX_BYTES - *len)
		ereport(ERROR, (errmsg("test application fixture is invalid")));
	memcpy(bytes + *len, body, strlen(body));
	*len += strlen(body);
	ctx = pg_cryptohash_create(PG_SHA256);
	if (!ctx || pg_cryptohash_init(ctx) < 0 || pg_cryptohash_update(ctx, (uint8 *)bytes, *len) < 0
		|| pg_cryptohash_final(ctx, ref->sha256, 32) < 0)
		ereport(ERROR, (errmsg("test application digest failed")));
	pg_cryptohash_free(ctx);
	return bytes;
}
#endif

void
_PG_init(void)
{
#ifdef USE_PGRAC_CLUSTER
	if (!process_shared_preload_libraries_in_progress || IsUnderPostmaster)
		return;
	DefineCustomIntVariable("test_pgrac_shared_config.apply_node", "Test startup application.",
							NULL, &test_apply_node, -1, -1, 127, PGC_POSTMASTER, 0, NULL, NULL,
							NULL);
	DefineCustomBoolVariable("test_pgrac_shared_config.bad_hash", "Test foreign object digest.",
							 NULL, &test_bad_hash, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("test_pgrac_shared_config.stop_after_apply",
							 "Stop before initialization.", NULL, &test_stop_after_apply, false,
							 PGC_POSTMASTER, 0, NULL, NULL, NULL);
	if (test_apply_node >= 0) {
		char path[MAXPGPATH];
		FILE *file;
		char *body = palloc(CLUSTER_SHARED_CONFIG_MAX_BYTES + 1);
		char *bytes;
		size_t len;
		ClusterSharedConfigRef ref;
		ClusterSharedConfigApplied applied;
		ResourceOwner saved_owner = CurrentResourceOwner;
		ResourceOwner fixture_owner;
		snprintf(path, sizeof(path), "%s/test_config.input", DataDir);
		file = AllocateFile(path, "rb");
		if (file == NULL)
			ereport(FATAL, (errmsg("could not open isolated application fixture")));
		len = fread(body, 1, CLUSTER_SHARED_CONFIG_MAX_BYTES + 1, file);
		if (ferror(file) || len > CLUSTER_SHARED_CONFIG_MAX_BYTES)
			ereport(FATAL, (errmsg("could not read isolated application fixture")));
		if (FreeFile(file) != 0)
			ereport(FATAL, (errmsg("could not close isolated application fixture")));
		body[len] = '\0';
		/* Creating test input is separate from consuming it. Restore the real
		 * early-postmaster NULL owner BEFORE invoking the product adapter.
		 */
		fixture_owner = ResourceOwnerCreate(NULL, "test configuration fixture");
		CurrentResourceOwner = fixture_owner;
		bytes = application_fixture(body, &ref, &len);
		CurrentResourceOwner = saved_owner;
		ResourceOwnerDelete(fixture_owner);
		if (test_bad_hash)
			ref.sha256[0] ^= 1;
		cluster_shared_config_apply_startup(bytes, len, &ref, test_apply_node, &applied);
		if (memcmp(&applied.ref, &ref, sizeof(ref)) != 0 || applied.node_id != test_apply_node)
			ereport(FATAL, (errmsg("test application receipt is not exact")));
		ereport(LOG,
				(errmsg("test shared configuration applied: node=%u entries=%u generation=%llu",
						applied.node_id, applied.applied_entries,
						(unsigned long long)applied.ref.identity.generation)));
		pfree(bytes);
		pfree(body);
		if (test_stop_after_apply)
			ereport(FATAL,
					(errmsg("test application completed; no storage initialization requested")));
	}
#endif
}

Datum
test_pgrac_config_backend_apply(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	ClusterSharedConfigRef ref;
	ClusterSharedConfigApplied applied;
	size_t len;
	char *bytes;
	if (!superuser())
		ereport(ERROR, (errmsg("test configuration inspection requires superuser")));
	bytes = application_fixture("common.statement_timeout='5s'\n", &ref, &len);
	cluster_shared_config_apply_startup(bytes, len, &ref, 0, &applied);
	PG_RETURN_BOOL(true);
#else
	PG_RETURN_BOOL(false);
#endif
}

Datum
test_pgrac_config_registration(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	bool old_phase = process_shared_preload_libraries_in_progress;
	bool old_done = process_shared_preload_libraries_done;
	if (!superuser())
		ereport(ERROR, (errmsg("test configuration inspection requires superuser")));
	process_cluster_gucs();
	process_cluster_gucs();
	PG_RETURN_BOOL(old_phase == process_shared_preload_libraries_in_progress
				   && old_done == process_shared_preload_libraries_done);
#else
	PG_RETURN_BOOL(false);
#endif
}

#ifdef USE_PGRAC_CLUSTER
static text *
result_text(ClusterControlRootResult rc, ClusterSharedConfigPolicyReport *report)
{
	return cstring_to_text(psprintf("%d:%d:%u:%u:%u:%s", rc == CLUSTER_CONTROL_ROOT_OK_PRIMARY,
									report->reason, report->checked_entries,
									report->restart_entries, report->cold_entries, report->name));
}
#endif

Datum
test_pgrac_config_entry(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	ClusterSharedConfigPolicyReport report;
	ClusterSharedConfigEntry entry;
	ClusterControlRootResult rc;
	if (!superuser())
		ereport(ERROR, (errmsg("test configuration inspection requires superuser")));
	entry.node_id = PG_GETARG_INT32(0);
	entry.name = text_to_cstring(PG_GETARG_TEXT_PP(1));
	entry.value = text_to_cstring(PG_GETARG_TEXT_PP(2));
	rc = cluster_shared_config_check_entry(&entry, PG_GETARG_BOOL(3), &report);
	PG_RETURN_TEXT_P(result_text(rc, &report));
#else
	ereport(ERROR, (errmsg("configuration inspection requires cluster build")));
	PG_RETURN_NULL();
#endif
}

Datum
test_pgrac_config_bootstrap(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	ClusterSharedConfigRef ref;
	ClusterSharedConfigPolicyReport report;
	ClusterControlRootResult result;
	char *bytes;
	size_t len;
	if (!superuser())
		ereport(ERROR, (errmsg("test configuration inspection requires superuser")));
	bytes = application_fixture(text_to_cstring(PG_GETARG_TEXT_PP(0)), &ref, &len);
	if (PG_GETARG_BOOL(5))
		ref.sha256[0] ^= 1;
	result = cluster_shared_config_check_bootstrap(
		bytes, len, &ref, PG_GETARG_INT32(1), text_to_cstring(PG_GETARG_TEXT_PP(2)),
		text_to_cstring(PG_GETARG_TEXT_PP(3)), text_to_cstring(PG_GETARG_TEXT_PP(4)), &report);
	PG_RETURN_TEXT_P(result_text(result, &report));
#else
	ereport(ERROR, (errmsg("configuration inspection requires cluster build")));
	PG_RETURN_NULL();
#endif
}

Datum
test_pgrac_config_object(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	ClusterSharedConfigRef ref = { 0 };
	ClusterSharedConfigPolicyReport report;
	ClusterSharedConfigStage stage;
	ClusterControlRootResult rc;
	char *bytes = palloc(CLUSTER_SHARED_CONFIG_MAX_BYTES);
	char *body = text_to_cstring(PG_GETARG_TEXT_PP(0));
	size_t len;
	pg_cryptohash_ctx *ctx;
	uint8 operation[16] = { 1 };
	if (!superuser())
		ereport(ERROR, (errmsg("test configuration inspection requires superuser")));
	ref.identity.system_identifier = 1234;
	ref.identity.database_incarnation = 1;
	ref.identity.generation = 1;
	ref.identity.configured[0] = 3;
	memset(ref.identity.storage_uuid, 1, 16);
	memset(ref.identity.authority_uuid, 2, 16);
	if (cluster_shared_config_encode(&ref.identity, NULL, 0, bytes, CLUSTER_SHARED_CONFIG_MAX_BYTES,
									 &len, ref.sha256)
			!= 0
		|| strlen(body) >= CLUSTER_SHARED_CONFIG_MAX_BYTES - len)
		ereport(ERROR, (errmsg("test configuration fixture is invalid")));
	memcpy(bytes + len, body, strlen(body));
	len += strlen(body);
	ctx = pg_cryptohash_create(PG_SHA256);
	if (!ctx || pg_cryptohash_init(ctx) < 0 || pg_cryptohash_update(ctx, (uint8 *)bytes, len) < 0
		|| pg_cryptohash_final(ctx, ref.sha256, 32) < 0)
		ereport(ERROR, (errmsg("test configuration digest failed")));
	pg_cryptohash_free(ctx);
	if (PG_GETARG_BOOL(1)) {
		/* The test uses only policy-invalid objects here. No path is created. */
		rc = cluster_shared_config_prepare_gucs("/pgrac-shared-config-test-must-not-exist", bytes,
												len, &ref, operation, &stage, &report);
		if (stage.state != 0)
			ereport(ERROR, (errmsg("invalid configuration unexpectedly reached staging")));
	} else
		rc = cluster_shared_config_check_gucs(bytes, len, &ref, &report);
	PG_RETURN_TEXT_P(result_text(rc, &report));
#else
	ereport(ERROR, (errmsg("configuration inspection requires cluster build")));
	PG_RETURN_NULL();
#endif
}
