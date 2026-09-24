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
#endif

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(test_pgrac_config_entry);
PG_FUNCTION_INFO_V1(test_pgrac_config_object);
PG_FUNCTION_INFO_V1(test_pgrac_config_registration);

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
