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
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_wal_claim.h"
#include "../../../backend/cluster/cluster_control_bootstrap_private.h"
#include "../../../backend/cluster/cluster_control_root_private.h"
#include "../../../backend/cluster/cluster_recovery_anchor_private.h"
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
PG_FUNCTION_INFO_V1(test_pgrac_bootstrap_fixture);
PG_FUNCTION_INFO_V1(test_pgrac_bootstrap_late);
PG_FUNCTION_INFO_V1(test_pgrac_bootstrap_control_late);
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
static bool test_prepare_bootstrap;
static int test_prepare_race;

static void
bootstrap_test_path(char path[MAXPGPATH], const char *suffix)
{
	if (snprintf(path, MAXPGPATH, "%s/test_native_bootstrap/%s", DataDir, suffix) >= MAXPGPATH)
		ereport(ERROR, (errmsg("test bootstrap path too long")));
}

static void
bootstrap_test_write(const char *suffix, const void *bytes, size_t len)
{
	char path[MAXPGPATH], parent[MAXPGPATH];
	FILE *file;
	bootstrap_test_path(path, suffix);
	strlcpy(parent, path, sizeof(parent));
	get_parent_directory(parent);
	if (pg_mkdir_p(parent, 0700) != 0)
		ereport(ERROR, (errmsg("cannot make test bootstrap directory: %m")));
	file = AllocateFile(path, "wb");
	if (!file || fwrite(bytes, 1, len, file) != len || FreeFile(file) != 0)
		ereport(ERROR, (errmsg("cannot write test bootstrap file: %m")));
}

static void
bootstrap_test_read(const char *suffix, void *bytes, size_t len)
{
	char path[MAXPGPATH];
	FILE *file;
	bootstrap_test_path(path, suffix);
	file = AllocateFile(path, "rb");
	if (!file || fread(bytes, 1, len, file) != len || FreeFile(file) != 0)
		ereport(ERROR, (errmsg("cannot read test bootstrap file: %m")));
}

static void
bootstrap_test_hash(const void *bytes, size_t len, uint8 hash[32])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	if (!ctx || pg_cryptohash_init(ctx) < 0 || pg_cryptohash_update(ctx, bytes, len) < 0
		|| pg_cryptohash_final(ctx, hash, 32) < 0)
		ereport(ERROR, (errmsg("test bootstrap hash failed")));
	pg_cryptohash_free(ctx);
}

static void
bootstrap_test_hex(const uint8 hash[32], char hex[65])
{
	for (int i = 0; i < 32; i++)
		snprintf(hex + i * 2, 3, "%02x", hash[i]);
}

/* Test-only native assign hook: deterministic replacement AFTER application,
 * never a substitute success predicate or a production injection point. */
static void
bootstrap_test_race(int value, void *extra)
{
	ResourceOwner saved = CurrentResourceOwner;
	ResourceOwner owner;
	if (value == 0)
		return;
	if (value == 3)
		ereport(ERROR, (errmsg("test bootstrap assignment exception")));
	owner = ResourceOwnerCreate(saved, "test bootstrap race");
	CurrentResourceOwner = owner;
	if (value == 1) {
		ControlRootImage *root = palloc0(sizeof(*root));
		uint8 storage[16];
		memset(storage, 1, sizeof(storage));
		bootstrap_test_read("shared/global/pgrac_control_root", root->bytes, sizeof(root->bytes));
		if (cluster_control_root_v2_decode(root->bytes, sizeof(root->bytes), storage,
										   GetSystemIdentifier(), root)
			!= 0)
			ereport(ERROR, (errmsg("test race root is invalid")));
		root->header.file_txn_seq++;
		if (cluster_control_root_v2_encode(root) != 0)
			ereport(ERROR, (errmsg("test race root cannot encode")));
		bootstrap_test_write("shared/global/pgrac_control_root", root->bytes, sizeof(root->bytes));
		pfree(root);
	} else if (value == 2) {
		uint8 bytes[256];
		PgracControlBinding binding;
		bootstrap_test_read("local/global/pgrac_control_binding", bytes, sizeof(bytes));
		if (!pgrac_control_binding_decode(bytes, sizeof(bytes), &binding))
			ereport(ERROR, (errmsg("test race binding is invalid")));
		binding.target_qualification_sha256[0] ^= 1;
		if (!pgrac_control_binding_encode(&binding, bytes, sizeof(bytes)))
			ereport(ERROR, (errmsg("test race binding cannot encode")));
		bootstrap_test_write("local/global/pgrac_control_binding", bytes, sizeof(bytes));
	}
	CurrentResourceOwner = saved;
	ResourceOwnerDelete(owner);
}

static void
bootstrap_test_prepare(void)
{
	char local[MAXPGPATH], shared[MAXPGPATH], wal[MAXPGPATH], undo[MAXPGPATH];
	ClusterControlBootstrapPrepared out;
	ResourceOwner saved = CurrentResourceOwner;
	uint64 sysid = GetSystemIdentifier();
	bootstrap_test_path(local, "local");
	bootstrap_test_path(shared, "shared");
	bootstrap_test_path(wal, "wal");
	bootstrap_test_path(undo, "undo");
	cluster_control_bootstrap_prepare(local, shared, wal, undo, 0, true, &out);
	if (out.snapshot.binding.system_identifier != sysid || out.snapshot.root_sequence != 7
		|| out.snapshot.config.identity.generation != 47 || out.applied.node_id != 0
		|| out.applied.ref.identity.generation != 47 || out.required.current_sources != 1
		|| out.required.max_connections != 300 || out.snapshot.control.MaxConnections != 300
		|| CurrentResourceOwner != saved || sysid != GetSystemIdentifier()
		|| (int)out.snapshot.control.xlog_seg_size != wal_segment_size
		|| memcmp(GetMockAuthenticationNonce(), out.snapshot.control.mock_authentication_nonce,
				  MOCK_AUTH_NONCE_LEN)
			   != 0
		|| DataChecksumsEnabled() != (out.snapshot.control.data_checksum_version != 0)
		|| strcmp(GetConfigOption("max_connections", false, false), "320") != 0
		|| strcmp(GetConfigOption("cluster.shared_data_dir", false, false), shared) != 0)
		ereport(FATAL, (errmsg("test native bootstrap result is not exact")));
	if (wal_segment_size == 128 * 1024 * 1024
		&& (XLOGbuffers != 4096 || !DataChecksumsEnabled()
			|| strcmp(GetConfigOption("data_checksums", false, false), "on") != 0))
		ereport(FATAL, (errmsg("test native bootstrap WAL sizing is not exact")));
	ereport(FATAL,
			(errmsg("test native bootstrap prepared; no admission or storage initialization")));
}

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
	DefineCustomBoolVariable("test_pgrac_shared_config.prepare_bootstrap",
							 "Test complete native preparation before storage.", NULL,
							 &test_prepare_bootstrap, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("cluster.native_bootstrap_test_race",
							"Test-only native assignment race in disposable objects.", NULL,
							&test_prepare_race, 0, 0, 3, PGC_POSTMASTER, 0, NULL,
							bootstrap_test_race, NULL);
	if (test_prepare_bootstrap)
		bootstrap_test_prepare();
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

/* A fixture constructor, NOT migration or qualification. Files live only in
 * this test extension's dedicated disposable directory under local PGDATA. */
Datum
test_pgrac_bootstrap_fixture(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	ControlRootImage *root;
	ControlRootHeader *header;
	ClusterControlRootSnapshot *record;
	ClusterRecoveryAnchorV2 anchor = { 0 };
	ClusterWalThreadClaimV2 claim = { 0 };
	ClusterSharedConfigIdentity config = { 0 };
	PgracControlBinding binding = { 0 };
	ControlFileData native;
	uint8 common[PG_CONTROL_FILE_SIZE], claim_bytes[112], anchor_bytes[512], binding_bytes[256];
	char shared[MAXPGPATH], wal[MAXPGPATH], undo[MAXPGPATH], suffix[MAXPGPATH], hex[65];
	char config_bytes[8192];
	size_t config_len;
	FILE *file;
	char *mutation;
	ClusterSharedConfigEntry entries[]
		= { { -1, "cluster.controlfile_shared_authority", "on" },
			{ -1, "cluster.enabled", "on" },
			{ -1, "cluster.merged_recovery", "on" },
			{ -1, "cluster.native_bootstrap_test_race", "0" },
			{ -1, "cluster.shared_catalog", "on" },
			{ -1, "cluster.shared_config", "on" },
			{ -1, "cluster.shared_data_dir", shared },
			{ -1, "cluster.shared_storage_backend", "cluster_fs" },
			{ -1, "cluster.shared_storage_uuid", "01010101-0101-0101-0101-010101010101" },
			{ -1, "cluster.smgr_user_relations", "on" },
			{ -1, "cluster.undo_tablespace_path", undo },
			{ -1, "cluster.wal_threads_dir", wal },
			{ -1, "max_connections", "320" },
			{ -1, "max_wal_size", "1024MB" },
			{ -1, "min_wal_size", "80MB" },
			{ -1, "wal_buffers", "-1" },
			{ 0, "cluster.node_id", "0" },
			{ 0, "shared_buffers", "128MB" } };
	if (!superuser())
		ereport(ERROR, (errmsg("test bootstrap fixture requires superuser")));
	mutation = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bootstrap_test_path(shared, "shared");
	bootstrap_test_path(wal, "wal");
	bootstrap_test_path(undo, "undo");
	file = AllocateFile(XLOG_CONTROL_FILE, "rb");
	if (!file || fread(&native, 1, sizeof(native), file) != sizeof(native) || FreeFile(file) != 0)
		ereport(ERROR, (errmsg("cannot read test native control")));
	/* Selected bytes differ from the compatibility image, without changing its
	 * identity. These fixture inputs never proceed to WAL/shared-memory startup. */
	native.mock_authentication_nonce[0] ^= 0x80;
	if (strcmp(mutation, "geometry") == 0 || strcmp(mutation, "wal-min") == 0
		|| strcmp(mutation, "wal-max") == 0) {
		native.xlog_seg_size = 128 * 1024 * 1024;
		native.data_checksum_version = 1;
		entries[13].value = strcmp(mutation, "wal-max") == 0 ? "64MB" : "512MB";
		entries[14].value = strcmp(mutation, "wal-min") == 0 ? "2MB" : "256MB";
		entries[17].value = "1GB";
	}
	root = palloc0(sizeof(*root));
	header = &root->header;
	header->format_version = 2;
	header->file_txn_seq = 7;
	header->system_identifier = native.system_identifier;
	memset(header->storage_uuid, 1, 16);
	memset(header->authority_uuid, 0xab, 16);
	header->authority_uuid[6] = 0x4b;
	header->authority_uuid[8] = 0x8b;
	header->activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED;
	header->created_at_usec = header->published_at_usec = 1;
	memset(header->migration_round_sha256, 0x11, 32);
	memset(header->source_wal_state_sha256, 0x22, 32);
	header->migration_prepare_generation = 3;
	header->migration_transition_epoch = 4;
	header->source_feature_bitmap = 1;
	header->target_feature_bitmap = PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	header->v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED;
	header->v2.database_incarnation = 41;
	header->v2.formation_seq = 43;
	header->v2.configured[0] = 1;
	header->v2.config_generation = 47;
	header->v2.control_image_generation = 53;
	header->v2.catalog_manifest_generation = 59;
	header->v2.global_scn_high_water = 61;
	memset(header->v2.catalog_manifest_sha256, 0x55, 32);
	config.system_identifier = native.system_identifier;
	config.database_incarnation = 41;
	config.generation = 47;
	config.configured[0] = 1;
	memcpy(config.storage_uuid, header->storage_uuid, 16);
	memcpy(config.authority_uuid, header->authority_uuid, 16);
	if (strcmp(mutation, "capacity") == 0)
		entries[12].value = "299";
	else if (strcmp(mutation, "profile") == 0)
		entries[0].value = "off";
	else if (strcmp(mutation, "root-race") == 0)
		entries[3].value = "1";
	else if (strcmp(mutation, "binding-race") == 0)
		entries[3].value = "2";
	else if (strcmp(mutation, "hook-error") == 0)
		entries[3].value = "3";
	if (cluster_shared_config_encode(&config, entries, lengthof(entries), config_bytes,
									 sizeof(config_bytes), &config_len, header->v2.config_sha256)
		!= 0)
		ereport(ERROR, (errmsg("test bootstrap config encoding failed")));
	bootstrap_test_hex(header->v2.config_sha256, hex);
	snprintf(suffix, sizeof(suffix), "shared/global/config_images/47-%s.conf", hex);
	bootstrap_test_write(suffix, config_bytes, config_len);
	if (strcmp(mutation, "checksum-version") == 0)
		native.data_checksum_version = 42;
	else if (strcmp(mutation, "native-format") == 0)
		native.blcksz++;
	if (cluster_cf_control_image_encode(&native, common) != 0)
		ereport(ERROR, (errmsg("test bootstrap common encoding failed")));
	bootstrap_test_hash(common, sizeof(common), header->v2.control_image_sha256);
	bootstrap_test_hex(header->v2.control_image_sha256, hex);
	snprintf(suffix, sizeof(suffix), "shared/global/control_images/53-%s.bin", hex);
	bootstrap_test_write(suffix, common, sizeof(common));
	record = &root->records[0];
	record->identity.system_identifier = native.system_identifier;
	memcpy(record->identity.storage_uuid, header->storage_uuid, 16);
	memcpy(record->identity.authority_uuid, header->authority_uuid, 16);
	record->identity.origin_thread_id = 1;
	record->identity.origin_node_id = 0;
	record->identity.origin_owner_incarnation = 99;
	record->identity.root_lineage_seq = 11;
	record->identity.thread_claim_created_at = 12345;
	claim.identity = record->identity;
	claim.database_incarnation = 41;
	claim.config_generation = 47;
	claim.claim_generation = 1;
	if (cluster_wal_claim_v2_encode(&claim, claim_bytes) != 0)
		ereport(ERROR, (errmsg("test bootstrap claim encoding failed")));
	for (int i = 0; i < 4; i++)
		record->identity.thread_claim_crc32c |= (uint32)claim_bytes[104 + i] << (8 * i);
	bootstrap_test_hash(claim_bytes, sizeof(claim_bytes), root->refs[0].claim_sha256);
	bootstrap_test_write("wal/thread_1/generation_99/pgrac_thread.claim", claim_bytes,
						 sizeof(claim_bytes));
	root->present[0] = true;
	root->publisher_incarnation[0] = 777;
	record->root_publish_seq = 10;
	record->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	record->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT;
	record->root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID;
	record->checkpoint_tli = native.checkPointCopy.ThisTimeLineID;
	record->checkpoint_lower_lsn = native.checkPointCopy.redo;
	record->checkpoint_record_crc32c = 1; /* fixture only; no WAL qualification */
	record->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	root->refs[0].anchor_generation = 66;
	anchor.identity = record->identity;
	anchor.database_incarnation = 41;
	anchor.config_generation = 47;
	anchor.anchor_generation = 66;
	memcpy(anchor.claim_sha256, root->refs[0].claim_sha256, 32);
	anchor.state = native.state;
	anchor.checkpoint = native.checkPoint;
	anchor.checkpoint_copy = native.checkPointCopy;
	anchor.write_time = native.time;
	anchor.unlogged_lsn = native.unloggedLSN;
	anchor.max_connections = 300;
	anchor.max_worker_processes = 1;
	anchor.max_locks_per_xact = 1;
	anchor.wal_level = native.wal_level;
	if (cluster_recovery_anchor_v2_encode(&anchor, anchor_bytes) != 0)
		ereport(ERROR, (errmsg("test bootstrap anchor encoding failed")));
	bootstrap_test_hash(anchor_bytes, sizeof(anchor_bytes), root->refs[0].anchor_sha256);
	bootstrap_test_hex(root->refs[0].anchor_sha256, hex);
	snprintf(suffix, sizeof(suffix),
			 "shared/global/anchor_images/thread_1/generation_99/anchor_66-%s.bin", hex);
	bootstrap_test_write(suffix, anchor_bytes, sizeof(anchor_bytes));
	if (cluster_control_root_v2_encode(root) != 0)
		ereport(ERROR, (errmsg("test bootstrap root encoding failed")));
	bootstrap_test_write("shared/global/pgrac_control_root", root->bytes, sizeof(root->bytes));
	binding.system_identifier = native.system_identifier;
	memcpy(binding.storage_uuid, header->storage_uuid, 16);
	memcpy(binding.authority_uuid, header->authority_uuid, 16);
	binding.database_incarnation = 41;
	memset(binding.operation_uuid, 0x41, 16);
	memset(binding.source_cold_sha256, 0x42, 32);
	memset(binding.target_qualification_sha256, 0x43, 32);
	memcpy(binding.migration_round_sha256, header->migration_round_sha256, 32);
	memcpy(binding.source_wal_state_sha256, header->source_wal_state_sha256, 32);
	binding.migration_prepare_generation = 3;
	binding.migration_transition_epoch = 4;
	if (strcmp(mutation, "identity") == 0)
		binding.database_incarnation++;
	if (!pgrac_control_binding_encode(&binding, binding_bytes, sizeof(binding_bytes)))
		ereport(ERROR, (errmsg("test bootstrap binding encoding failed")));
	bootstrap_test_write("local/global/pgrac_control_binding", binding_bytes,
						 sizeof(binding_bytes));
	pfree(root);
	PG_RETURN_BOOL(true);
#else
	PG_RETURN_BOOL(false);
#endif
}

Datum
test_pgrac_bootstrap_late(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	ClusterControlBootstrapPrepared out;
	if (!superuser())
		ereport(ERROR, (errmsg("test bootstrap inspection requires superuser")));
	cluster_control_bootstrap_prepare(NULL, NULL, NULL, NULL, 0, true, &out);
#endif
	PG_RETURN_BOOL(false);
}

Datum
test_pgrac_bootstrap_control_late(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test native control inspection requires superuser")));
	if (PG_GETARG_BOOL(0))
		XLogInstallBootstrapControlFile(NULL, true);
	else
		XLogCompleteBootstrapControlFile();
	PG_RETURN_BOOL(false);
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
