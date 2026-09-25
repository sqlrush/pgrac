/* PGRAC: test-only native GUC engine integration, no product catalog entry.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
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
PGDLLEXPORT void _PG_init(void);

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
