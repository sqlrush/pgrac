/* PGRAC: test-only native GUC engine integration, no product catalog entry.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include <sys/stat.h>
#include <unistd.h>
#include "access/parallel.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_wal_claim.h"
#include "cluster/cluster_wal_durable_prefix.h"
#include "cluster/cluster_wal_thread.h"
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
PG_FUNCTION_INFO_V1(test_pgrac_config_change);
PG_FUNCTION_INFO_V1(test_pgrac_config_reload);
PG_FUNCTION_INFO_V1(test_pgrac_config_process);
PG_FUNCTION_INFO_V1(test_pgrac_config_parallel_observe);
PG_FUNCTION_INFO_V1(test_pgrac_config_enrollment);
PG_FUNCTION_INFO_V1(test_pgrac_config_native_role);
PG_FUNCTION_INFO_V1(test_pgrac_config_slot_probe);
PG_FUNCTION_INFO_V1(test_pgrac_config_registration);
PG_FUNCTION_INFO_V1(test_pgrac_config_selection_cleanup);
PG_FUNCTION_INFO_V1(test_pgrac_config_delivery);
PG_FUNCTION_INFO_V1(test_pgrac_config_delivery_state);
PG_FUNCTION_INFO_V1(test_pgrac_config_delivery_refuse);
PG_FUNCTION_INFO_V1(test_pgrac_config_delivery_receive);
PG_FUNCTION_INFO_V1(test_pgrac_config_census);
PG_FUNCTION_INFO_V1(test_pgrac_config_active);
PG_FUNCTION_INFO_V1(test_pgrac_config_define_common);
PG_FUNCTION_INFO_V1(test_pgrac_config_active_census);
PG_FUNCTION_INFO_V1(test_pgrac_config_backend_apply);
PG_FUNCTION_INFO_V1(test_pgrac_config_bootstrap);
PG_FUNCTION_INFO_V1(test_pgrac_control_image);
PG_FUNCTION_INFO_V1(test_pgrac_recovery_capacity);
PG_FUNCTION_INFO_V1(test_pgrac_bootstrap_fixture);
PG_FUNCTION_INFO_V1(test_pgrac_bootstrap_late);
PG_FUNCTION_INFO_V1(test_pgrac_bootstrap_control_late);
PGDLLEXPORT void _PG_init(void);
extern void test_pgrac_config_work_init(void);

Datum
test_pgrac_config_define_common(PG_FUNCTION_ARGS)
{
	static bool defined;
	static bool value;
	if (!superuser())
		ereport(ERROR, (errmsg("test native registry inspection requires superuser")));
	if (!defined) {
		DefineCustomBoolVariable("cluster.native_config_late", "Test native registration", NULL,
								 &value, false, PGC_SUSET, 0, NULL, NULL, NULL);
		defined = true;
	}
	PG_RETURN_VOID();
}

Datum
test_pgrac_config_active_census(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test native active census requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigProcess actual;
		ClusterSharedConfigCensus census;
		if (!cluster_shared_config_process_observe(&actual)
			|| !cluster_shared_config_node_census(&actual.ref, actual.node_id, &census))
			PG_RETURN_TEXT_P(cstring_to_text("unavailable"));
		if (census.participants != census.current_processes)
			PG_RETURN_TEXT_P(cstring_to_text("not-current"));
		PG_RETURN_TEXT_P(cstring_to_text(
			psprintf("%u:%u:%u:%u", census.active.version, census.active_missing_processes,
					 census.static_mismatch_processes, census.dynamic_mismatch_processes)));
	}
#else
	PG_RETURN_TEXT_P(cstring_to_text("unavailable"));
#endif
}

/* Actual native values or the actual backend's published observation. This
 * does not mutate a value, grant permission, or fabricate a process role.
 * Author: SqlRush <sqlrush@gmail.com>
 */
Datum
test_pgrac_config_active(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test active configuration inspection requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigActive active;
		ClusterSharedConfigRegistration registration;
		char static_hex[65], dynamic_hex[65];
		static const char hex[] = "0123456789abcdef";
		if (PG_GETARG_BOOL(0)) {
			if (MyProc == NULL
				|| !cluster_shared_config_registration_read(&MyProc->cluster_config, &registration))
				PG_RETURN_TEXT_P(cstring_to_text("unavailable"));
			active = registration.active;
		} else if (!cluster_shared_config_active_profile(&active))
			PG_RETURN_TEXT_P(cstring_to_text("unavailable"));
		if (active.version != CLUSTER_SHARED_CONFIG_ACTIVE_VERSION)
			PG_RETURN_TEXT_P(cstring_to_text("unavailable"));
		for (int i = 0; i < 32; ++i) {
			static_hex[2 * i] = hex[active.static_sha256[i] >> 4];
			static_hex[2 * i + 1] = hex[active.static_sha256[i] & 15];
			dynamic_hex[2 * i] = hex[active.dynamic_sha256[i] >> 4];
			dynamic_hex[2 * i + 1] = hex[active.dynamic_sha256[i] & 15];
		}
		static_hex[64] = dynamic_hex[64] = '\0';
		PG_RETURN_TEXT_P(
			cstring_to_text(psprintf("%u:%u:%u:%s:%s", active.version, active.static_entries,
									 active.dynamic_entries, static_hex, dynamic_hex)));
	}
#else
	PG_RETURN_TEXT_P(cstring_to_text("unavailable"));
#endif
}

/* Actual native context accounting, deliberately without CF authority.
 * Positive selected-file/ERROR timing is tested in cluster_unit; this test
 * cannot substitute for a distributed grant or fabricate one.
 * Author: SqlRush <sqlrush@gmail.com>
 */
Datum
test_pgrac_config_selection_cleanup(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	MemoryContext parent, caller;
	ClusterSharedConfigRef prior = { 0 };
	Size before;
	bool ok = true;
	if (!superuser())
		ereport(ERROR, (errmsg("test config selection inspection requires superuser")));
	parent = AllocSetContextCreate(CurrentMemoryContext, "selection test parent",
								   ALLOCSET_DEFAULT_SIZES);
	caller = MemoryContextSwitchTo(parent);
	before = MemoryContextMemAllocated(parent, true);
	prior.identity.generation = prior.identity.system_identifier = 1;
	prior.identity.storage_uuid[0] = 1;
	for (int i = 0; i < 1000; ++i) {
		ClusterSharedConfigSelected selected;
		ClusterSharedConfigImage image;
		ClusterSharedConfigSelected zero = { 0 };
		CHECK_FOR_INTERRUPTS();
		ok &= cluster_control_root_config_read_locked(&prior, &selected, &image)
			  == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		ok &= CurrentMemoryContext == parent && parent->firstchild == NULL;
		ok &= memcmp(&selected, &zero, sizeof(zero)) == 0 && image.bytes == NULL && image.len == 0;
		ok &= MemoryContextMemAllocated(parent, true) == before;
	}
	MemoryContextSwitchTo(caller);
	MemoryContextDelete(parent);
	PG_RETURN_BOOL(ok);
#else
	ereport(ERROR, (errmsg("cluster support is not compiled")));
	PG_RETURN_BOOL(false);
#endif
}

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
static int test_process_generation;
static int test_process_failure;
static bool test_process_defer;
static bool test_process_delivery;
static int test_logger_failure;

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
bootstrap_test_native_inputs(const char *mutation)
{
	static const char *const directories[]
		= { "local/pg_twophase", "local/pg_replslot", "local/pg_logical/snapshots",
			"local/pg_logical/mappings" };
	static const char *const files[]
		= { "local/standby.signal", "local/pg_twophase/0000002A",
			"local/pg_replslot/unfinished.tmp", "local/pg_logical/replorigin_checkpoint" };
	static const char *const cases[]
		= { "input-signal", "input-2pc", "input-slot", "input-progress" };
	const char marker[] = "retained unsupported native input";
	char path[MAXPGPATH];
	for (size_t i = 0; i < lengthof(directories); ++i) {
		bootstrap_test_path(path, directories[i]);
		if (pg_mkdir_p(path, 0700) != 0)
			ereport(ERROR, (errmsg("cannot make test native input directory: %m")));
	}
	for (size_t i = 0; i < lengthof(files); ++i) {
		bootstrap_test_path(path, files[i]);
		if (unlink(path) != 0 && errno != ENOENT)
			ereport(ERROR, (errmsg("cannot reset test native input: %m")));
		if (strcmp(mutation, cases[i]) == 0)
			bootstrap_test_write(files[i], marker, sizeof(marker));
	}
}

static void
bootstrap_test_side_routes(const char *mutation)
{
	static const char *const families[]
		= { "pg_xact", "pg_subtrans", "pg_multixact", "pg_commit_ts" };
	char path[MAXPGPATH], alias[MAXPGPATH], suffix[MAXPGPATH];
	struct stat st;
	for (size_t i = 0; i < lengthof(families); ++i) {
		snprintf(suffix, sizeof(suffix), "shared/native_side/origin_0/%s", families[i]);
		bootstrap_test_path(path, suffix);
		if (pg_mkdir_p(path, 0700) != 0)
			ereport(ERROR, (errmsg("cannot make test native side directory: %m")));
		snprintf(suffix, sizeof(suffix), "local/%s", families[i]);
		bootstrap_test_path(alias, suffix);
		if (lstat(alias, &st) == 0) {
			if ((S_ISDIR(st.st_mode) ? rmdir(alias) : unlink(alias)) != 0)
				ereport(ERROR, (errmsg("cannot reset test native side alias: %m")));
		} else if (errno != ENOENT)
			ereport(ERROR, (errmsg("cannot inspect test native side alias: %m")));
		if (i == 0 && strcmp(mutation, "side-missing") == 0)
			continue;
		if (i == 0 && strcmp(mutation, "side-literal") == 0) {
			if (mkdir(alias, 0700) != 0)
				ereport(ERROR, (errmsg("cannot make test local side directory: %m")));
			continue;
		}
		if (i == 0 && strcmp(mutation, "side-foreign") == 0)
			bootstrap_test_path(path, "shared");
		if (symlink(path, alias) != 0)
			ereport(ERROR, (errmsg("cannot make test native side alias: %m")));
	}
	for (unsigned i = 0; i < 2; ++i) {
		snprintf(suffix, sizeof(suffix), "shared/native_side/origin_0/pg_multixact/%s",
				 i == 0 ? "offsets" : "members");
		bootstrap_test_path(path, suffix);
		if (lstat(path, &st) == 0 && S_ISLNK(st.st_mode) && unlink(path) != 0)
			ereport(ERROR, (errmsg("cannot reset test MX child: %m")));
		if (pg_mkdir_p(path, 0700) != 0)
			ereport(ERROR, (errmsg("cannot create test MX child: %m")));
		if (i == 0 && strcmp(mutation, "side-mx-symlink") == 0) {
			bootstrap_test_path(alias, "shared");
			if (rmdir(path) != 0 || symlink(alias, path) != 0)
				ereport(ERROR, (errmsg("cannot substitute test MX child: %m")));
		}
	}
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
	if (value == 0 || value >= 4)
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
	ClusterWalDurablePrefixRef wal_ref;
	ResourceOwner saved = CurrentResourceOwner;
	uint64 sysid = GetSystemIdentifier();
	bootstrap_test_path(local, "local");
	bootstrap_test_path(shared, "shared");
	bootstrap_test_path(wal, "wal");
	bootstrap_test_path(undo, "undo");
	memset(&wal_ref, 0xa5, sizeof(wal_ref));
	if (cluster_wal_thread_current_v2_ref(&wal_ref)
		|| memcmp(&wal_ref, &(ClusterWalDurablePrefixRef){ 0 }, sizeof(wal_ref)) != 0)
		ereport(FATAL, (errmsg("test uninitialized WAL reference was exposed")));
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
	if (test_prepare_race == 4)
		bootstrap_test_race(1, NULL);
	else if (test_prepare_race == 5)
		bootstrap_test_race(2, NULL);
	else if (test_prepare_race == 6 || test_prepare_race == 7 || test_prepare_race == 9) {
		char target[MAXPGPATH];
		bootstrap_test_path(target, test_prepare_race == 9 ? "local/pg_xact"
									: test_prepare_race == 6
										? "local/pg_wal"
										: "wal/thread_1/generation_99/durable_prefix/current");
		if (unlink(target) != 0)
			ereport(FATAL, (errmsg("test WAL recheck unlink failed")));
	}
	cluster_control_bootstrap_wal_recheck(test_prepare_race == 8 ? DataDir : local, &wal_ref);
	if (CurrentResourceOwner != saved || wal_ref.claim.identity.origin_thread_id != 1
		|| wal_ref.claim.identity.origin_owner_incarnation != 99
		|| wal_ref.claim.database_incarnation != 41 || wal_ref.claim.max_config_generation != 47
		|| wal_ref.timeline != out.snapshot.control.checkPointCopy.ThisTimeLineID
		|| memcmp(&wal_ref, &out.snapshot.wal, sizeof(wal_ref)) != 0)
		ereport(FATAL, (errmsg("test WAL bootstrap recheck result is not exact")));
	ereport(FATAL,
			(errmsg("test native bootstrap prepared; no admission or storage initialization")));
}

/* The test creates exact bytes, but production code performs all validation
 * and application. No fake native process identity or mutable engine flags.
 */
static char *
application_fixture_generation(const char *body, uint64 generation, ClusterSharedConfigRef *ref,
							   size_t *len)
{
	char *bytes = palloc(CLUSTER_SHARED_CONFIG_MAX_BYTES);
	pg_cryptohash_ctx *ctx;
	memset(ref, 0, sizeof(*ref));
	ref->identity.system_identifier = 1234;
	ref->identity.database_incarnation = 1;
	ref->identity.generation = generation;
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

static char *
application_fixture(const char *body, ClusterSharedConfigRef *ref, size_t *len)
{
	return application_fixture_generation(body, 1, ref, len);
}

static ClusterControlRootResult
reload_seed(const ClusterSharedConfigEntry *entry, void *arg)
{
	int node_id = *(int *)arg;
	if (entry->node_id != -1 && entry->node_id != node_id)
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	/* This is only setup in a disposable backend; use actual SIGHUP semantics,
	 * never pretend to be a postmaster to install an otherwise forbidden value.
	 * Cold negative fixtures use the real immutable node id. */
	(void)set_config_option(entry->name, entry->value, PGC_SIGHUP, PGC_S_FILE, GUC_ACTION_SET, true,
							DEBUG1, false);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static int reload_test_value;

/* Real registered assign-hook failure; exists only in this test extension. */
static void
reload_test_assign(int value, void *extra)
{
	if (value == 2)
		ereport(ERROR, (errmsg("test native reload assignment failure")));
}

static void
logger_test_assign(int value, void *extra)
{
	if (value == 2 && IsUnderPostmaster && MyBackendType == B_LOGGER)
		ereport(ERROR, (errmsg("test detached logger assignment failure")));
}

/* Test-only driver into the actual consumer in a real postmaster/child.
 * This does not select a CF root, publish an ACK or change process roles. */
static void
process_test_reload(int generation, void *extra)
{
	char path[MAXPGPATH];
	char *body, *bytes;
	size_t len;
	FILE *file;
	ResourceOwner saved_owner = CurrentResourceOwner;
	ResourceOwner owner;
	ClusterSharedConfigRef ref;
	ClusterSharedConfigProcess out;
	ClusterSharedConfigPolicyReport report;
	ClusterControlRootResult result;

	if (generation < 2 || test_process_defer)
		return;
	if (test_process_delivery && IsUnderPostmaster) {
		(void)cluster_shared_config_delivery_reload();
		return;
	}
	snprintf(path, sizeof(path), "%s/test_config.reload", DataDir);
	body = palloc(CLUSTER_SHARED_CONFIG_MAX_BYTES + 1);
	file = AllocateFile(path, "rb");
	if (!file)
		ereport(ERROR, (errmsg("cannot open process reload fixture")));
	len = fread(body, 1, CLUSTER_SHARED_CONFIG_MAX_BYTES + 1, file);
	if (ferror(file) || len > CLUSTER_SHARED_CONFIG_MAX_BYTES || FreeFile(file) != 0)
		ereport(ERROR, (errmsg("cannot read process reload fixture")));
	body[len] = '\0';
	owner = ResourceOwnerCreate(saved_owner, "test process reload fixture");
	CurrentResourceOwner = owner;
	bytes = application_fixture_generation(body, generation, &ref, &len);
	CurrentResourceOwner = saved_owner;
	ResourceOwnerDelete(owner);
	result = cluster_shared_config_process_reload(bytes, len, &ref, &out, &report);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && test_process_delivery && !IsUnderPostmaster)
		(void)cluster_shared_config_delivery_parent_publish();
	ereport(LOG, (errmsg("test process configuration consume: generation=%d ok=%d pid=%d",
						 generation, result == 0, MyProcPid)));
	pfree(bytes);
	pfree(body);
}
#endif

void
_PG_init(void)
{
#ifdef USE_PGRAC_CLUSTER
	if (!process_shared_preload_libraries_in_progress || IsUnderPostmaster)
		return;
	test_pgrac_config_work_init();
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
							&test_prepare_race, 0, 0, 9, PGC_POSTMASTER, 0, NULL,
							bootstrap_test_race, NULL);
	DefineCustomIntVariable("test_pgrac_shared_config.reload_generation",
							"Test-only native process reload.", NULL, &test_process_generation, 0,
							0, 100, PGC_SIGHUP, 0, NULL, process_test_reload, NULL);
	DefineCustomBoolVariable("test_pgrac_shared_config.defer_process",
							 "Test-only delayed process application.", NULL, &test_process_defer,
							 false, PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("test_pgrac_shared_config.delivery",
							 "Test-only native family delivery with injected selected source.",
							 NULL, &test_process_delivery, false, PGC_POSTMASTER, 0, NULL, NULL,
							 NULL);
	DefineCustomIntVariable("cluster.native_config_process_failure",
							"Test-only partial process application.", NULL, &test_process_failure,
							0, 0, 2, PGC_SIGHUP, 0, NULL, reload_test_assign, NULL);
	DefineCustomIntVariable(
		"cluster.native_config_logger_failure", "Test-only detached logger assignment failure.",
		NULL, &test_logger_failure, 0, 0, 2, PGC_SIGHUP, 0, NULL, logger_test_assign, NULL);
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
		if (test_process_delivery)
			cluster_shared_config_delivery_start();
		if (test_stop_after_apply)
			ereport(FATAL,
					(errmsg("test application completed; no storage initialization requested")));
	}
#endif
}

Datum
test_pgrac_config_delivery(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test delivery inspection requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigRegistration out;
		if (!cluster_shared_config_delivery_logger_observe(&out))
			PG_RETURN_TEXT_P(cstring_to_text("none"));
		PG_RETURN_TEXT_P(cstring_to_text(
			psprintf("%llu:%llu:%d:%d", (unsigned long long)out.process.ref.identity.generation,
					 (unsigned long long)out.registration, out.pid, out.role == B_LOGGER)));
	}
#else
	PG_RETURN_TEXT_P(cstring_to_text("disabled"));
#endif
}

Datum
test_pgrac_config_delivery_state(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test delivery inspection requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigRegistration out;
		if (!cluster_shared_config_delivery_logger_observe(&out))
			PG_RETURN_TEXT_P(cstring_to_text("none"));
		PG_RETURN_TEXT_P(cstring_to_text(
			psprintf("%llu:%d", (unsigned long long)out.process.ref.identity.generation,
					 out.process.failed)));
	}
#else
	PG_RETURN_TEXT_P(cstring_to_text("disabled"));
#endif
}

Datum
test_pgrac_config_census(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test configuration census requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigProcess actual;
		ClusterSharedConfigCensus out;
		if (!cluster_shared_config_process_observe(&actual)
			|| !cluster_shared_config_node_census(&actual.ref, actual.node_id, &out))
			PG_RETURN_TEXT_P(cstring_to_text("busy"));
		/* Test-only view of an actual census. This process's actual inherited
		 * target is not a CF selection or member/common-value admission. */
		PG_RETURN_TEXT_P(cstring_to_text(psprintf(
			"%u:%u:%u:%u:%u:%u:%u:%llu:%llu", out.participants, out.current_processes,
			out.waiting_processes, out.failed_processes, out.parallel_processes,
			out.pending_processes, out.deferred_processes, (unsigned long long)out.pending_entries,
			(unsigned long long)out.deferred_entries)));
	}
#else
	PG_RETURN_TEXT_P(cstring_to_text("disabled"));
#endif
}

Datum
test_pgrac_config_delivery_receive(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test delivery requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	/* Invoke the real consumer in the caller's actual native transaction.
	 * Do not synthesize an idle state or an application outcome. */
	PG_RETURN_BOOL(cluster_shared_config_delivery_reload());
#else
	PG_RETURN_BOOL(false);
#endif
}

Datum
test_pgrac_config_delivery_refuse(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test delivery requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigProcess process;
		ClusterSharedConfigImage image;
		bool refused;
		if (!cluster_shared_config_process_copy(&process, &image))
			PG_RETURN_BOOL(false);
		refused = !cluster_shared_config_delivery_parent_publish()
				  && !cluster_shared_config_delivery_publish(&process.ref, &image);
		cluster_shared_config_free(&image);
		PG_RETURN_BOOL(refused);
	}
#else
	PG_RETURN_BOOL(false);
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
	ClusterWalDurablePrefixRef prefix_ref = { 0 };
	ClusterWalDurablePrefix prefix = { .sequence = 1 };
	ClusterSharedConfigIdentity config = { 0 };
	PgracControlBinding binding = { 0 };
	ControlFileData native;
	uint8 common[PG_CONTROL_FILE_SIZE], claim_bytes[112], anchor_bytes[512], binding_bytes[256];
	uint8 prefix_bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
	char pgwal[MAXPGPATH], generation[MAXPGPATH];
	char shared[MAXPGPATH], wal[MAXPGPATH], undo[MAXPGPATH], suffix[MAXPGPATH], hex[65];
	char config_bytes[8192];
	size_t config_len;
	size_t entry_count;
	FILE *file;
	char *mutation;
	bool native_entry;
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
	native_entry = strncmp(mutation, "entry-", 6) == 0;
	if (native_entry)
		mutation += 6;
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
	else if (strcmp(mutation, "recheck-root") == 0)
		entries[3].value = "4";
	else if (strcmp(mutation, "recheck-binding") == 0)
		entries[3].value = "5";
	else if (strcmp(mutation, "recheck-route") == 0)
		entries[3].value = "6";
	else if (strcmp(mutation, "recheck-prefix") == 0)
		entries[3].value = "7";
	else if (strcmp(mutation, "recheck-pgdata") == 0)
		entries[3].value = "8";
	else if (strcmp(mutation, "recheck-side") == 0)
		entries[3].value = "9";
	entry_count = lengthof(entries);
	if (native_entry) {
		/* Actual LocalProcessControlFile precedes test-library registration.
		 * Omit the test-only assign-hook entry from these initial inputs. */
		memmove(&entries[3], &entries[4], (entry_count - 4) * sizeof(entries[0]));
		entry_count--;
	}
	if (cluster_shared_config_encode(&config, entries, entry_count, config_bytes,
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
	/* Routing/prefix fixture only, not a never-written or physical WAL proof. */
	prefix_ref.claim.identity = record->identity;
	prefix_ref.claim.database_incarnation = 41;
	prefix_ref.claim.max_config_generation = 47;
	memcpy(prefix_ref.claim.claim_sha256, root->refs[0].claim_sha256, 32);
	prefix_ref.timeline = native.checkPointCopy.ThisTimeLineID;
	if (strcmp(mutation, "prefix-identity") == 0)
		prefix_ref.timeline++;
	if (cluster_wal_durable_prefix_encode(&prefix_ref, &prefix, prefix_bytes) != 0)
		ereport(ERROR, (errmsg("test bootstrap prefix encoding failed")));
	if (strcmp(mutation, "prefix-corrupt") == 0)
		prefix_bytes[140] ^= 1;
	bootstrap_test_write("wal/thread_1/generation_99/durable_prefix/current", prefix_bytes,
						 sizeof(prefix_bytes));
	if (strcmp(mutation, "prefix-missing") == 0) {
		bootstrap_test_path(suffix, "wal/thread_1/generation_99/durable_prefix/current");
		if (unlink(suffix) != 0)
			ereport(ERROR, (errmsg("test prefix unlink failed")));
	}
	bootstrap_test_path(pgwal, "local/pg_wal");
	bootstrap_test_path(generation, strcmp(mutation, "wal-flat") == 0
										? "wal/thread_1"
										: "wal/thread_1/generation_99");
	if (unlink(pgwal) != 0 && errno != ENOENT)
		ereport(ERROR, (errmsg("test pg_wal unlink failed")));
	if (strcmp(mutation, "wal-missing") != 0 && symlink(generation, pgwal) != 0)
		ereport(ERROR, (errmsg("test pg_wal symlink failed")));
	bootstrap_test_side_routes(mutation);
	bootstrap_test_native_inputs(mutation);
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

/* PGRAC: actual policy/staging in this standalone TAP node's private fixture.
 * Does not replace globals, select a shared root or publish configuration.
 * Author: SqlRush <sqlrush@gmail.com> */
Datum
test_pgrac_config_change(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test configuration change requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigRef ref;
		ClusterSharedConfigEntry change;
		ClusterSharedConfigStage stage;
		ClusterSharedConfigPolicyReport report;
		ClusterControlRootResult result;
		ClusterSharedConfigImage image = { 0 };
		uint8 operation[16] = { 1 };
		char root[MAXPGPATH], path[MAXPGPATH];
		char *bytes, *answer;
		size_t len;
		bool changed = false;
		FILE *file;
		uint32 count;
		if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(4))
			ereport(ERROR, (errmsg("test change arguments missing")));
		bytes = application_fixture(text_to_cstring(PG_GETARG_TEXT_PP(0)), &ref, &len);
		change.node_id = PG_GETARG_INT32(1);
		change.name = text_to_cstring(PG_GETARG_TEXT_PP(2));
		change.value = PG_ARGISNULL(3) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(3));
		if (snprintf(root, sizeof(root), "%s/test_config_change-XXXXXX", DataDir) >= sizeof(root)
			|| mkdtemp(root) == NULL)
			ereport(ERROR, (errmsg("cannot create private change fixture")));
		snprintf(path, sizeof(path), "%s/global/config_images/.staging", root);
		if (pg_mkdir_p(path, 0700) != 0)
			ereport(ERROR, (errmsg("cannot create private staging fixture")));
		if (PG_GETARG_BOOL(4))
			ref.sha256[0] ^= 1;
		result = cluster_shared_config_prepare_change(root, bytes, len, &ref, &change, operation,
													  &stage, &changed, &report);
		answer = psprintf("%d:%u:%d:%u", result == 0, report.reason, changed, stage.state);
		snprintf(path, sizeof(path),
				 "%s/global/config_images/.staging/01000000000000000000000000000000.tmp", root);
		if (result == 0 && changed) {
			image.bytes = palloc(stage.bytes + 1);
			file = AllocateFile(path, "rb");
			if (file == NULL || fread(image.bytes, 1, stage.bytes, file) != stage.bytes
				|| fgetc(file) != EOF || FreeFile(file) != 0)
				ereport(ERROR, (errmsg("staged configuration bytes missing or wrong length")));
			image.len = stage.bytes;
			image.bytes[image.len] = '\0';
			if (stage.ref.identity.generation != ref.identity.generation + 1
				|| cluster_shared_config_validate(image.bytes, image.len, &stage.ref, &count) != 0)
				ereport(ERROR, (errmsg("staged configuration is not the exact new object")));
			if (cluster_shared_config_discard(root, &stage) != 0)
				ereport(ERROR, (errmsg("private staged configuration did not retire")));
			cluster_shared_config_free(&image);
		} else if (stage.state != 0 || changed || access(path, F_OK) == 0)
			ereport(ERROR, (errmsg("refusal or no-op unexpectedly reached staging")));
		snprintf(path, sizeof(path), "%s/global/config_images/.staging", root);
		if (rmdir(path) != 0)
			ereport(ERROR, (errmsg("private staging fixture was not empty")));
		snprintf(path, sizeof(path), "%s/global/config_images", root);
		if (rmdir(path) != 0)
			ereport(ERROR, (errmsg("private object fixture was not empty")));
		snprintf(path, sizeof(path), "%s/global", root);
		if (rmdir(path) != 0 || rmdir(root) != 0)
			ereport(ERROR, (errmsg("private change fixture cleanup failed")));
		PG_RETURN_TEXT_P(cstring_to_text(answer));
	}
#else
	ereport(ERROR, (errmsg("configuration change requires cluster build")));
	PG_RETURN_NULL();
#endif
}

/* PGRAC: actual backend reload and GUC stacks, no activation/ACK substitution.
 * Author: SqlRush <sqlrush@gmail.com> */
Datum
test_pgrac_config_reload(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test configuration reload requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigRef old_ref, new_ref;
		ClusterSharedConfigReload applied;
		ClusterSharedConfigPolicyReport report;
		ClusterControlRootResult result;
		char *old_bytes, *new_bytes;
		size_t old_len, new_len;
		int node_id = PG_GETARG_INT32(2);
		char *fault = text_to_cstring(PG_GETARG_TEXT_PP(3));
		ResourceOwner saved_owner = CurrentResourceOwner;
		MemoryContext saved_context = CurrentMemoryContext;
		volatile bool threw = false;
		if (strcmp(fault, "assign") == 0)
			DefineCustomIntVariable(
				"cluster.native_config_reload_failure", "Disposable native assignment failure.",
				NULL, &reload_test_value, 0, 0, 2, PGC_SIGHUP, 0, NULL, reload_test_assign, NULL);
		old_bytes = application_fixture_generation(text_to_cstring(PG_GETARG_TEXT_PP(0)),
												   strcmp(fault, "backwards") == 0 ? 4 : 1,
												   &old_ref, &old_len);
		new_bytes = application_fixture_generation(text_to_cstring(PG_GETARG_TEXT_PP(1)),
												   strcmp(fault, "equal") == 0 ? 1 : 3, &new_ref,
												   &new_len);
		if (cluster_shared_config_visit(old_bytes, old_len, &old_ref, reload_seed, &node_id) != 0)
			ereport(ERROR, (errmsg("invalid old reload fixture")));
		if (strcmp(fault, "session") == 0 || strcmp(fault, "local") == 0)
			(void)set_config_option("work_mem", "21MB", PGC_USERSET, PGC_S_SESSION,
									strcmp(fault, "local") == 0 ? GUC_ACTION_LOCAL : GUC_ACTION_SET,
									true, ERROR, false);
		if (strcmp(fault, "oldhash") == 0)
			old_ref.sha256[0] ^= 1;
		if (strcmp(fault, "newhash") == 0)
			new_ref.sha256[0] ^= 1;
		memset(&applied, 0xa5, sizeof(applied));
		PG_TRY();
		{
			result
				= cluster_shared_config_apply_reload(old_bytes, old_len, &old_ref, new_bytes,
													 new_len, &new_ref, node_id, &applied, &report);
		}
		PG_CATCH();
		{
			ErrorData *error;
			MemoryContextSwitchTo(saved_context);
			if (strcmp(fault, "assign") != 0)
				PG_RE_THROW();
			error = CopyErrorData();
			if (strcmp(error->message, "test native reload assignment failure") != 0)
				PG_RE_THROW();
			FreeErrorData(error);
			FlushErrorState();
			threw = true;
		}
		PG_END_TRY();
		if (threw) {
			result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			report.reason = CLUSTER_CONFIG_POLICY_VALUE;
		}
		if (CurrentResourceOwner != saved_owner || CurrentMemoryContext != saved_context)
			ereport(ERROR, (errmsg("reload leaked its native owner or memory context")));
		if (result != 0
			&& memcmp(&applied, &(ClusterSharedConfigReload){ 0 }, sizeof(applied)) != 0)
			ereport(ERROR, (errmsg("failed reload exposed an application receipt")));
		if (result == 0
			&& (memcmp(&applied.old_ref, &old_ref, sizeof(old_ref)) != 0
				|| memcmp(&applied.ref, &new_ref, sizeof(new_ref)) != 0
				|| applied.node_id != node_id))
			ereport(ERROR, (errmsg("reload application receipt is not exact")));
		if (strcmp(fault, "totals") == 0)
			PG_RETURN_TEXT_P(
				cstring_to_text(psprintf("%d:%u:%u:%u", result == 0, report.reason,
										 applied.pending_restart_total, applied.deferred_total)));
		PG_RETURN_TEXT_P(cstring_to_text(psprintf(
			"%d:%u:%u:%u:%u:%u", result == 0, report.reason, applied.applied_entries,
			applied.removed_entries, applied.pending_restart_entries, applied.deferred_entries)));
	}
#else
	ereport(ERROR, (errmsg("configuration reload requires cluster build")));
	PG_RETURN_NULL();
#endif
}

/* Persistent process state, never an externally supplied old configuration. */
Datum
test_pgrac_config_process(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test process inspection requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		int generation = PG_GETARG_INT32(0);
		ClusterSharedConfigProcess state, out;
		ClusterSharedConfigRef ref;
		ClusterSharedConfigPolicyReport report;
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		char *prefix = "";
		if (generation != 0) {
			size_t len;
			char *bytes = application_fixture_generation(text_to_cstring(PG_GETARG_TEXT_PP(1)),
														 generation, &ref, &len);
			memset(&out, 0xa5, sizeof(out));
			result = cluster_shared_config_process_reload(bytes, len, &ref, &out, &report);
			if (result != 0 && memcmp(&out, &(ClusterSharedConfigProcess){ 0 }, sizeof(out)))
				ereport(ERROR, (errmsg("failed process apply exposed a receipt")));
			if (result == 0 && (out.failed || memcmp(&out.ref, &ref, sizeof(ref))))
				ereport(ERROR, (errmsg("process receipt not bound to applied target")));
			prefix = result == 0 ? "ok:" : "refused:";
		}
		if (!cluster_shared_config_process_observe(&state))
			PG_RETURN_TEXT_P(cstring_to_text(psprintf("%sunseeded", prefix)));
		PG_RETURN_TEXT_P(cstring_to_text(
			psprintf("%s%llu:%u:%u:%d:%d", prefix,
					 (unsigned long long)state.ref.identity.generation, state.pending_restart_total,
					 state.deferred_total, state.failed, state.applier_pid == (int32)getppid())));
	}
#else
	ereport(ERROR, (errmsg("process inspection requires cluster build")));
	PG_RETURN_NULL();
#endif
}

/* Read-only observation suitable for actual native parallel workers. */
Datum
test_pgrac_config_parallel_observe(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test parallel inspection requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigProcess state;
		bool observed = cluster_shared_config_process_observe(&state);
		PG_RETURN_TEXT_P(cstring_to_text(psprintf("%d:%d:%llu:%s", IsParallelWorker(), observed,
												  (unsigned long long)state.ref.identity.generation,
												  GetConfigOption("work_mem", false, false))));
	}
#else
	PG_RETURN_TEXT_P(cstring_to_text("plain"));
#endif
}

/* Diagnostics only: native slots, never fabricate a process or target. */
Datum
test_pgrac_config_enrollment(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test enrollment inspection requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigRegistration state;
		int pid = PG_GETARG_INT32(0);
		int index = ProcGlobal->allProcCount;
		ClusterSharedConfigSlot *slot = &ProcGlobal->cluster_config_postmaster;
		if (pid != -1) {
			for (index = 0; index < ProcGlobal->allProcCount; ++index)
				if (ProcGlobal->allProcs[index].pid == pid)
					break;
			if (index == ProcGlobal->allProcCount)
				PG_RETURN_TEXT_P(cstring_to_text("absent"));
			slot = &ProcGlobal->allProcs[index].cluster_config;
		}
		if (!cluster_shared_config_registration_read(slot, &state))
			PG_RETURN_TEXT_P(cstring_to_text("unavailable"));
		/* A freed native PGPROC can retain its old pid. The registration's
		 * real exit is the observation, not the stale diagnostic pid field. */
		if (state.pid == 0)
			PG_RETURN_TEXT_P(cstring_to_text("absent"));
		if (pid != -1 && state.pid != pid)
			PG_RETURN_TEXT_P(cstring_to_text("unavailable"));
		PG_RETURN_TEXT_P(cstring_to_text(
			psprintf("%d:%llu:%llu:%d:%d", index, (unsigned long long)state.registration,
					 (unsigned long long)state.process.ref.identity.generation,
					 state.process.failed, state.process.parallel_snapshot)));
	}
#else
	PG_RETURN_TEXT_P(cstring_to_text("plain"));
#endif
}

/* Actual native role observation only; never synthesize process enrollment. */
Datum
test_pgrac_config_native_role(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test native role inspection requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigRegistration state;
		int pid = PG_GETARG_INT32(0);
		bool found = false;
		if (pid == -1)
			found = cluster_shared_config_registration_read(&ProcGlobal->cluster_config_postmaster,
															&state);
		else if (pid == -2)
			found = cluster_shared_config_delivery_logger_observe(&state);
		else {
			int32 *pids = palloc(sizeof(*pids) * ProcGlobal->allProcCount);
			if (ProcConfigSnapshotPids(pids, ProcGlobal->allProcCount))
				for (unsigned i = 0; i < ProcGlobal->allProcCount; ++i)
					if (pids[i] == pid && pid > 0) {
						found = cluster_shared_config_registration_read(
									&ProcGlobal->allProcs[i].cluster_config, &state)
								&& state.pid == pid;
						break;
					}
			pfree(pids);
		}
		if (!found || state.pid <= 0 || state.registration == 0)
			PG_RETURN_TEXT_P(cstring_to_text("unavailable"));
		PG_RETURN_TEXT_P(cstring_to_text(psprintf("%d:%d", state.role, state.aux_type)));
	}
#else
	PG_RETURN_TEXT_P(cstring_to_text("plain"));
#endif
}

/* Slot boundary tests complement (not replace) the real process TAP. */
Datum
test_pgrac_config_slot_probe(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test slot inspection requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigSlot slot;
		ClusterSharedConfigRegistration out, before;
		int mode = PG_GETARG_INT32(0);
		bool result;
		if (mode == 4) {
			ClusterSharedConfigSlot *previous = palloc(sizeof(*previous));
			ClusterSharedConfigSlot *replacement = palloc(sizeof(*replacement));
			ClusterSharedConfigProcess original, current;
			bool attached, first;
			if (!cluster_shared_config_process_observe(&original))
				ereport(ERROR, (errmsg("relocation test needs an actual native image")));
			cluster_shared_config_process_detach();
			cluster_shared_config_registration_init(previous);
			first = cluster_shared_config_process_attach(previous);
			/* Distinct simultaneously live allocations deterministically model
			 * a relocated replacement mapping; no address-reuse assumption. */
			cluster_shared_config_process_new_shmem();
			cluster_shared_config_registration_init(replacement);
			attached = cluster_shared_config_process_attach(replacement);
			result = first && attached && cluster_shared_config_process_observe(&current)
					 && memcmp(&original, &current, sizeof(original)) == 0;
			cluster_shared_config_process_detach();
			if (!cluster_shared_config_process_attach(&MyProc->cluster_config))
				ereport(ERROR, (errmsg("could not restore actual native test registration")));
			pfree(previous);
			pfree(replacement);
			PG_RETURN_BOOL(result);
		}
		cluster_shared_config_registration_init(&slot);
		slot.value.pid = 17;
		slot.value.registration = 2;
		slot.value.observed = true;
		before = slot.value;
		memset(&out, 0xa5, sizeof(out));
		if (mode == 2) {
			result = cluster_shared_config_registration_read(&slot, &slot.value);
			PG_RETURN_BOOL(!result && memcmp(&before, &slot.value, sizeof(before)) == 0);
		}
		if (mode == 1 || mode == 3)
			pg_atomic_write_u64(&slot.sequence, mode == 1 ? 1 : PG_UINT64_MAX);
		result = cluster_shared_config_registration_read(&slot, &out);
		if (mode == 0)
			PG_RETURN_BOOL(result && memcmp(&before, &out, sizeof(out)) == 0);
		PG_RETURN_BOOL(!result
					   && memcmp(&out, &(ClusterSharedConfigRegistration){ 0 }, sizeof(out)) == 0);
	}
#else
	PG_RETURN_BOOL(false);
#endif
}
