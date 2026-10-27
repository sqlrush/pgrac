/*-------------------------------------------------------------------------
 *
 * test_cluster_fresh_scope.c
 *    Native startup and index entry points enforce the fresh-database scope.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_fresh_scope.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/amapi.h"
#include "access/heaptoast.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/catversion.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_control.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "commands/user.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_tt_local.h"
#include "commands/tablecmds.h"
#include "common/controldata_utils.h"
#include "miscadmin.h"
#include "nodes/value.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "storage/large_object.h"
#include "storage/lock.h"
#include "tcop/utility.h"
#include "utils/rel.h"
#include "../../backend/cluster/cluster_control_bootstrap_private.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
const ObjectAddress InvalidObjectAddress = { 0 };
bool cluster_shared_config, cluster_shared_catalog;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
bool IsUnderPostmaster, IsPostmasterEnvironment = true;
bool process_shared_preload_libraries_done;
ProcessingMode Mode = NormalProcessing;
int cluster_node_id;
char *DataDir, *cluster_shared_data_dir, *cluster_wal_threads_dir;
char *cluster_undo_tablespace_path;
static ControlFileData *ControlFile;
static sigjmp_buf boundary;
static int error_code;
static char error_message[256], error_hint[256];
static unsigned prepare_calls, legacy_calls, open_calls, write_opens;
static PgracControlBindingResult binding_result = PGRAC_CONTROL_BINDING_MISSING;

PgracControlBindingResult
pgrac_control_binding_read(const char *pgdata, PgracControlBinding *out)
{
	(void)pgdata;
	memset(out, 0, sizeof(*out));
	return binding_result;
}

void *
palloc(Size n)
{
	return malloc(n);
}
void
pfree(void *p)
{
	free(p);
}
void
check_stack_depth(void)
{}
/* Dynamic rewrite classification has native SQL tests; this fixture covers
 * only the preceding raw utility routing and index entry points. */
void
CheckClusterSharedAlterTable(AlterTableStmt *stmt, const char *queryString)
{
	(void)stmt;
	(void)queryString;
}
void
ExceptionalCondition(const char *c, const char *f, int line)
{
	fprintf(stderr, "%s %s:%d\n", c, f, line);
	abort();
}
bool
errstart(int level, const char *domain)
{
	(void)domain;
	return level >= ERROR;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
void
errfinish(const char *file, int line, const char *fn)
{
	(void)file;
	(void)line;
	(void)fn;
	siglongjmp(boundary, 1);
}
int
errcode(int code)
{
	error_code = code;
	return 0;
}
int
errcode_for_file_access(void)
{
	error_code = ERRCODE_IO_ERROR;
	return 0;
}
int
errmsg(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(error_message, sizeof(error_message), fmt, ap);
	va_end(ap);
	return 0;
}
int
errmsg_plural(const char *singular, const char *plural, unsigned long n, ...)
{
	(void)singular;
	(void)plural;
	(void)n;
	return 0;
}
int
errhint(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(error_hint, sizeof(error_hint), fmt, ap);
	va_end(ap);
	return 0;
}
int
errdetail(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}
int
parser_errposition(ParseState *pstate, int location)
{
	(void)pstate;
	(void)location;
	return 0;
}
int
OpenTransientFile(const char *path, int flags)
{
	open_calls++;
	if ((flags & O_ACCMODE) != O_RDONLY || (flags & (O_CREAT | O_TRUNC)) != 0)
		write_opens++;
	return open(path, flags, 0600);
}
int
CloseTransientFile(int fd)
{
	return close(fd);
}
void
process_cluster_gucs(void)
{}
void
cluster_control_bootstrap_prepare(const char *a, const char *b, const char *c, const char *d,
								  uint32 node, bool reset, ClusterControlBootstrapPrepared *out)
{
	(void)a;
	(void)b;
	(void)c;
	(void)d;
	(void)node;
	(void)reset;
	(void)out;
	prepare_calls++; /* This boundary must never be reached by PRE1 input. */
}
static void
ReadControlFile(void)
{
	legacy_calls++; /* Non-shared native route is not replaced. */
}
void
PreventInTransactionBlock(bool top, const char *stmt)
{
	(void)top;
	(void)stmt;
}
static bool utility_in_transaction;
static bool utility_storage_mode = true;
bool XactReadOnly;
static bool utility_in_child, utility_parallel, utility_bind_ok = true;
static bool utility_expect_binding;
static TransactionId utility_top_xid, utility_current_xid;
static unsigned utility_xids, utility_binds, utility_prepares, utility_unbound;

bool
IsSubTransaction(void)
{
	return utility_in_child;
}

bool
IsInParallelMode(void)
{
	return utility_parallel;
}

bool
RecoveryInProgress(void)
{
	return false;
}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return utility_top_xid;
}

TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return utility_current_xid;
}

TransactionId
GetTopTransactionId(void)
{
	if (!TransactionIdIsValid(utility_top_xid)) {
		utility_top_xid = utility_current_xid = 400;
		utility_xids++;
	}
	return utility_top_xid;
}

bool
cluster_tt_local_prepare_canonical_active(TransactionId xid, ClusterCanonicalTxnBinding *binding)
{
	utility_prepares++;
	UT_ASSERT_EQ(xid, 400);
	if (!utility_bind_ok)
		return false;
	if (utility_binds == 0)
		utility_binds++;
	MemSet(binding, 0, sizeof(*binding));
	binding->xid = xid;
	binding->segment_id = 1;
	return true;
}

CommandTag
CreateCommandTag(Node *statement)
{
	if (IsA(statement, TruncateStmt))
		return CMDTAG_TRUNCATE_TABLE;
	if (IsA(statement, CreateSchemaStmt))
		return CMDTAG_CREATE_SCHEMA;
	if (IsA(statement, CreateSeqStmt))
		return CMDTAG_CREATE_SEQUENCE;
	if (IsA(statement, IndexStmt))
		return CMDTAG_CREATE_INDEX;
	if (IsA(statement, ViewStmt))
		return CMDTAG_CREATE_VIEW;
	/* This boundary models the table rename/schema-move cases below. */
	if (IsA(statement, RenameStmt) || IsA(statement, AlterObjectSchemaStmt))
		return CMDTAG_ALTER_TABLE;
	if (IsA(statement, GrantStmt))
		return ((GrantStmt *)statement)->is_grant ? CMDTAG_GRANT : CMDTAG_REVOKE;
	if (IsA(statement, GrantRoleStmt))
		return ((GrantRoleStmt *)statement)->is_grant ? CMDTAG_GRANT_ROLE : CMDTAG_REVOKE_ROLE;
	if (IsA(statement, AlterDefaultPrivilegesStmt))
		return CMDTAG_ALTER_DEFAULT_PRIVILEGES;
	if (IsA(statement, CreateFunctionStmt))
		return ((CreateFunctionStmt *)statement)->is_procedure ? CMDTAG_CREATE_PROCEDURE
															   : CMDTAG_CREATE_FUNCTION;
	if (IsA(statement, AlterFunctionStmt)) {
		switch (((AlterFunctionStmt *)statement)->objtype) {
		case OBJECT_FUNCTION:
			return CMDTAG_ALTER_FUNCTION;
		case OBJECT_PROCEDURE:
			return CMDTAG_ALTER_PROCEDURE;
		case OBJECT_ROUTINE:
			return CMDTAG_ALTER_ROUTINE;
		default:
			return CMDTAG_UNKNOWN;
		}
	}
	if (IsA(statement, CreateTrigStmt))
		return CMDTAG_CREATE_TRIGGER;
	if (IsA(statement, RuleStmt))
		return CMDTAG_CREATE_RULE;
	if (IsA(statement, CreatePolicyStmt))
		return CMDTAG_CREATE_POLICY;
	if (IsA(statement, AlterPolicyStmt))
		return CMDTAG_ALTER_POLICY;
	if (IsA(statement, CommentStmt))
		return CMDTAG_COMMENT;
	if (IsA(statement, CreateRoleStmt))
		return CMDTAG_CREATE_ROLE;
	if (IsA(statement, AlterRoleStmt) || IsA(statement, AlterRoleSetStmt))
		return CMDTAG_ALTER_ROLE;
	if (IsA(statement, DropRoleStmt))
		return CMDTAG_DROP_ROLE;
	if (IsA(statement, AlterDatabaseSetStmt) || IsA(statement, AlterDatabaseRefreshCollStmt))
		return CMDTAG_ALTER_DATABASE;
	if (IsA(statement, CreatePLangStmt))
		return CMDTAG_CREATE_LANGUAGE;
	if (IsA(statement, CreateConversionStmt))
		return CMDTAG_CREATE_CONVERSION;
	if (IsA(statement, CreateCastStmt))
		return CMDTAG_CREATE_CAST;
	if (IsA(statement, CreateTransformStmt))
		return CMDTAG_CREATE_TRANSFORM;
	if (IsA(statement, CreateAmStmt))
		return CMDTAG_CREATE_ACCESS_METHOD;
	if (IsA(statement, AlterSeqStmt))
		return CMDTAG_ALTER_SEQUENCE;
	if (IsA(statement, AlterTSDictionaryStmt))
		return CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY;
	if (IsA(statement, AlterTSConfigurationStmt))
		return CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION;
	if (IsA(statement, CreateStatsStmt))
		return CMDTAG_CREATE_STATISTICS;
	if (IsA(statement, AlterStatsStmt))
		return CMDTAG_ALTER_STATISTICS;
	if (IsA(statement, CreateEventTrigStmt))
		return CMDTAG_CREATE_EVENT_TRIGGER;
	if (IsA(statement, AlterEventTrigStmt))
		return CMDTAG_ALTER_EVENT_TRIGGER;
	if (IsA(statement, AlterCollationStmt))
		return CMDTAG_ALTER_COLLATION;
	if (IsA(statement, DefineStmt)) {
		switch (((DefineStmt *)statement)->kind) {
		case OBJECT_AGGREGATE:
			return CMDTAG_CREATE_AGGREGATE;
		case OBJECT_COLLATION:
			return CMDTAG_CREATE_COLLATION;
		case OBJECT_TSPARSER:
			return CMDTAG_CREATE_TEXT_SEARCH_PARSER;
		case OBJECT_TSDICTIONARY:
			return CMDTAG_CREATE_TEXT_SEARCH_DICTIONARY;
		case OBJECT_TSTEMPLATE:
			return CMDTAG_CREATE_TEXT_SEARCH_TEMPLATE;
		case OBJECT_TSCONFIGURATION:
			return CMDTAG_CREATE_TEXT_SEARCH_CONFIGURATION;
		default:
			return CMDTAG_UNKNOWN;
		}
	}
	return IsA(statement, CreateTableAsStmt) ? CMDTAG_CREATE_TABLE_AS : CMDTAG_CREATE_TABLE;
}

const char *
GetCommandTagName(CommandTag tag)
{
	if (tag == CMDTAG_TRUNCATE_TABLE)
		return "TRUNCATE TABLE";
	if (tag == CMDTAG_CREATE_SCHEMA)
		return "CREATE SCHEMA";
	if (tag == CMDTAG_CREATE_SEQUENCE)
		return "CREATE SEQUENCE";
	if (tag == CMDTAG_CREATE_INDEX)
		return "CREATE INDEX";
	if (tag == CMDTAG_CREATE_VIEW)
		return "CREATE VIEW";
	if (tag == CMDTAG_ALTER_TABLE)
		return "ALTER TABLE";
	if (tag == CMDTAG_GRANT)
		return "GRANT";
	if (tag == CMDTAG_REVOKE)
		return "REVOKE";
	if (tag == CMDTAG_GRANT_ROLE)
		return "GRANT ROLE";
	if (tag == CMDTAG_REVOKE_ROLE)
		return "REVOKE ROLE";
	if (tag == CMDTAG_ALTER_DEFAULT_PRIVILEGES)
		return "ALTER DEFAULT PRIVILEGES";
	if (tag == CMDTAG_CREATE_FUNCTION)
		return "CREATE FUNCTION";
	if (tag == CMDTAG_CREATE_PROCEDURE)
		return "CREATE PROCEDURE";
	if (tag == CMDTAG_ALTER_FUNCTION)
		return "ALTER FUNCTION";
	if (tag == CMDTAG_ALTER_PROCEDURE)
		return "ALTER PROCEDURE";
	if (tag == CMDTAG_ALTER_ROUTINE)
		return "ALTER ROUTINE";
	if (tag == CMDTAG_CREATE_TRIGGER)
		return "CREATE TRIGGER";
	if (tag == CMDTAG_CREATE_RULE)
		return "CREATE RULE";
	if (tag == CMDTAG_CREATE_POLICY)
		return "CREATE POLICY";
	if (tag == CMDTAG_ALTER_POLICY)
		return "ALTER POLICY";
	if (tag == CMDTAG_COMMENT)
		return "COMMENT";
	if (tag == CMDTAG_CREATE_ROLE)
		return "CREATE ROLE";
	if (tag == CMDTAG_ALTER_ROLE)
		return "ALTER ROLE";
	if (tag == CMDTAG_DROP_ROLE)
		return "DROP ROLE";
	if (tag == CMDTAG_ALTER_DATABASE)
		return "ALTER DATABASE";
	if (tag == CMDTAG_CREATE_LANGUAGE)
		return "CREATE LANGUAGE";
	if (tag == CMDTAG_CREATE_CONVERSION)
		return "CREATE CONVERSION";
	if (tag == CMDTAG_CREATE_CAST)
		return "CREATE CAST";
	if (tag == CMDTAG_CREATE_TRANSFORM)
		return "CREATE TRANSFORM";
	if (tag == CMDTAG_CREATE_ACCESS_METHOD)
		return "CREATE ACCESS METHOD";
	if (tag == CMDTAG_ALTER_SEQUENCE)
		return "ALTER SEQUENCE";
	if (tag == CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY)
		return "ALTER TEXT SEARCH DICTIONARY";
	if (tag == CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION)
		return "ALTER TEXT SEARCH CONFIGURATION";
	if (tag == CMDTAG_CREATE_STATISTICS)
		return "CREATE STATISTICS";
	if (tag == CMDTAG_ALTER_STATISTICS)
		return "ALTER STATISTICS";
	if (tag == CMDTAG_CREATE_EVENT_TRIGGER)
		return "CREATE EVENT TRIGGER";
	if (tag == CMDTAG_ALTER_EVENT_TRIGGER)
		return "ALTER EVENT TRIGGER";
	if (tag == CMDTAG_ALTER_COLLATION)
		return "ALTER COLLATION";
	if (tag == CMDTAG_CREATE_AGGREGATE)
		return "CREATE AGGREGATE";
	if (tag == CMDTAG_CREATE_COLLATION)
		return "CREATE COLLATION";
	if (tag == CMDTAG_CREATE_TEXT_SEARCH_PARSER)
		return "CREATE TEXT SEARCH PARSER";
	if (tag == CMDTAG_CREATE_TEXT_SEARCH_DICTIONARY)
		return "CREATE TEXT SEARCH DICTIONARY";
	if (tag == CMDTAG_CREATE_TEXT_SEARCH_TEMPLATE)
		return "CREATE TEXT SEARCH TEMPLATE";
	if (tag == CMDTAG_CREATE_TEXT_SEARCH_CONFIGURATION)
		return "CREATE TEXT SEARCH CONFIGURATION";
	return tag == CMDTAG_CREATE_TABLE_AS ? "CREATE TABLE AS" : "CREATE TABLE";
}

extern bool cluster_storage_mode_enabled(void);

bool
cluster_storage_mode_enabled(void)
{
	return utility_storage_mode;
}
bool
IsInTransactionBlock(bool top)
{
	return utility_in_transaction || !top;
}
char *
defGetString(DefElem *def)
{
	if (IsA(def->arg, String))
		return strVal(def->arg);
	abort();
}

#include "test_cluster_fresh_control.inc"
#include "test_cluster_fresh_read.inc"
#include "test_cluster_fresh_startup.inc"
#include "test_cluster_fresh_boolean.inc"
static unsigned storage_drop_preflights, utility_after_preflight;
static bool drop_target_shared;
void
CheckRelationsStorageDrop(DropStmt *stmt, bool isTopLevel)
{
	storage_drop_preflights++;
	if (drop_target_shared) {
		error_code = utility_in_transaction ? ERRCODE_ACTIVE_SQL_TRANSACTION
										 : ERRCODE_FEATURE_NOT_SUPPORTED;
		siglongjmp(boundary, 1);
	}
}
void
CheckObjectsStorageDrop(DropStmt *stmt, bool isTopLevel)
{
	CheckRelationsStorageDrop(stmt, isTopLevel);
}
void
CheckOwnedObjectsStorageDrop(DropOwnedStmt *stmt, bool isTopLevel)
{
	CheckRelationsStorageDrop(NULL, isTopLevel);
}

#include "test_cluster_shared_utility.inc"
#include "test_cluster_fresh_index.inc"
#include "test_cluster_fresh_catalog_index.inc"

static void
sql_entry(Node *node)
{
	PlannedStmt plan = { 0 };
	PlannedStmt *pstmt = &plan;
	Node *parsetree;
	const char *queryString = "";
	bool isTopLevel = true;
	plan.utilityStmt = node;
#include "test_cluster_fresh_utility.inc"
	(void)parsetree;
	utility_after_preflight++;
}

static void
create_sql_entry(Node *node)
{
	PlannedStmt plan = { 0 };
	PlannedStmt *pstmt = &plan;
	Node *parsetree;
	const char *queryString = "";
	bool isTopLevel = true;
	int readonly_flags;

	plan.utilityStmt = node;
#include "test_cluster_create_utility_entry.inc"
	if (utility_expect_binding && utility_binds == 0)
		utility_unbound++;
	utility_after_preflight++;
}

static void
reset_error(void)
{
	error_code = 0;
	error_message[0] = error_hint[0] = '\0';
}

UT_TEST(test_pre1_startup_refuses_before_root_and_without_writes)
{
	char tmp[] = "/tmp/pgrac-fresh-scope-XXXXXX";
	char global[MAXPGPATH], path[MAXPGPATH];
	ControlFileData image = { 0 }, after;
	pg_crc32c crc;
	int fd;
	int caught;

	UT_ASSERT(mkdtemp(tmp) != NULL);
	snprintf(global, sizeof(global), "%s/global", tmp);
	snprintf(path, sizeof(path), "%s/pg_control", global);
	UT_ASSERT_EQ(mkdir(global, 0700), 0);
	image.pg_control_version = PG_CONTROL_VERSION;
	image.catalog_version_no = 202609120;
	image.data_checksum_version = PG_DATA_CHECKSUM_VERSION;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &image, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	image.crc = crc;
	fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, &image, sizeof(image)), sizeof(image));
	UT_ASSERT_EQ(close(fd), 0);
	DataDir = tmp;
	for (int mode = 1; mode <= 2; ++mode) {
		cluster_shared_config = mode == 1;
		cluster_shared_catalog = mode == 2;
		open_calls = write_opens = 0;
		reset_error();
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			LocalProcessControlFile(false);
		UT_ASSERT_EQ(caught, 1);
		UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT(strstr(error_message, "PRE1") != NULL);
		UT_ASSERT(strstr(error_hint, "new database") != NULL);
		UT_ASSERT_EQ(prepare_calls, 0);
		UT_ASSERT_EQ(legacy_calls, 0);
		UT_ASSERT_EQ(open_calls, 1);
		UT_ASSERT_EQ(write_opens, 0);
	}
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(read(fd, &after, sizeof(after)), sizeof(after));
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(rmdir(global), 0);
	UT_ASSERT_EQ(rmdir(tmp), 0);
	DataDir = NULL;
	UT_ASSERT(memcmp(&image, &after, sizeof(image)) == 0);
}

UT_TEST(test_selected_pre1_image_and_corruption_are_distinct)
{
	ControlFileData image = { 0 };
	pg_crc32c crc;
	int caught;
	image.pg_control_version = PG_CONTROL_VERSION;
	image.catalog_version_no = 202609120;
	image.data_checksum_version = PG_DATA_CHECKSUM_VERSION;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &image, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	image.crc = crc;
	cluster_shared_config = true;
	reset_error();
	caught = sigsetjmp(boundary, 1);
	if (!caught)
		XLogValidateControlFile(&image);
	UT_ASSERT_EQ(caught, 1);
	UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	image.crc ^= 1;
	reset_error();
	caught = sigsetjmp(boundary, 1);
	if (!caught)
		XLogValidateControlFile(&image);
	UT_ASSERT_EQ(caught, 1);
	UT_ASSERT(strstr(error_message, "checksum") != NULL);
	UT_ASSERT(error_code != ERRCODE_FEATURE_NOT_SUPPORTED);
}

UT_TEST(test_nonshared_startup_remains_native)
{
	cluster_shared_config = false;
	cluster_shared_catalog = false;
	ControlFile = NULL;
	LocalProcessControlFile(false);
	UT_ASSERT_EQ(legacy_calls, 1);
	free(ControlFile);
	ControlFile = NULL;
}

UT_TEST(test_catalog_only_profile_refuses_pre1)
{
	ControlFileData image = { 0 };
	pg_crc32c crc;
	int caught;

	image.pg_control_version = PG_CONTROL_VERSION;
	image.catalog_version_no = 202609120;
	image.data_checksum_version = PG_DATA_CHECKSUM_VERSION;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &image, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	image.crc = crc;
	cluster_shared_config = false;
	cluster_shared_catalog = true;
	reset_error();
	caught = sigsetjmp(boundary, 1);
	if (!caught)
		XLogValidateControlFile(&image);
	UT_ASSERT_EQ(caught, 1);
	UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT(strstr(error_message, "PRE1") != NULL);
}

UT_TEST(test_bound_startup_does_not_consult_compatibility_projection)
{
	int caught;
	cluster_shared_config = true;
	cluster_shared_catalog = false;
	binding_result = PGRAC_CONTROL_BINDING_OK;
	DataDir = "/nonexistent-pgrac-test-projection";
	prepare_calls = open_calls = write_opens = 0;
	reset_error();
	caught = sigsetjmp(boundary, 1);
	if (!caught)
		LocalProcessControlFile(false);
	UT_ASSERT_EQ(caught, 0);
	UT_ASSERT_EQ(prepare_calls, 1);
	UT_ASSERT_EQ(open_calls, 0);
	UT_ASSERT_EQ(write_opens, 0);
	DataDir = NULL;
	binding_result = PGRAC_CONTROL_BINDING_MISSING;
}

static ControlFileData
checksum_control(uint32 version, DBState state)
{
	ControlFileData image = { 0 };
	pg_crc32c crc;
	image.pg_control_version = PG_CONTROL_VERSION;
	image.catalog_version_no = CATALOG_VERSION_NO;
	image.maxAlign = MAXIMUM_ALIGNOF;
	image.floatFormat = FLOATFORMAT_VALUE;
	image.blcksz = BLCKSZ;
	image.relseg_size = RELSEG_SIZE;
	image.xlog_blcksz = XLOG_BLCKSZ;
	image.nameDataLen = NAMEDATALEN;
	image.indexMaxKeys = INDEX_MAX_KEYS;
	image.toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
	image.loblksize = LOBLKSIZE;
#ifdef USE_FLOAT8_BYVAL
	image.float8ByVal = true;
#endif
	image.xlog_seg_size = 16 * 1024 * 1024;
	image.state = state;
	image.data_checksum_version = version;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &image, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	image.crc = crc;
	return image;
}

UT_TEST(test_shared_selected_control_requires_supported_checksums)
{
	const DBState states[] = { DB_SHUTDOWNED, DB_IN_PRODUCTION, DB_IN_CRASH_RECOVERY };
	cluster_shared_config = true;
	for (unsigned i = 0; i < lengthof(states); i++)
		for (uint32 version = 0; version <= PG_DATA_CHECKSUM_VERSION + 1; version++) {
			ControlFileData image = checksum_control(version, states[i]);
			ControlFileData before = image;
			int caught;
			reset_error();
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				XLogValidateControlFile(&image);
			/* Atomic format activation is still a separate owner.  A valid
			 * checksum must preserve the current PRE1 refusal until then. */
			UT_ASSERT_EQ(caught,
						 version != PG_DATA_CHECKSUM_VERSION || CATALOG_VERSION_NO == 202609120);
			if (version != PG_DATA_CHECKSUM_VERSION) {
				UT_ASSERT_EQ(error_code, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
				UT_ASSERT(strstr(error_message, "requires data checksums") != NULL);
				UT_ASSERT(strstr(error_hint, "initdb -k") != NULL);
			} else if (CATALOG_VERSION_NO == 202609120) {
				UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
				UT_ASSERT(strstr(error_message, "PRE1") != NULL);
			}
			UT_ASSERT(memcmp(&image, &before, sizeof(image)) == 0);
		}
}
UT_TEST(test_shared_unbound_checksum_refusal_precedes_root_and_writes)
{
	char tmp[] = "/tmp/pgrac-checksum-scope-XXXXXX";
	char global[MAXPGPATH], path[MAXPGPATH];
	ControlFileData image = checksum_control(0, DB_SHUTDOWNED), after;
	int fd, caught;

	UT_ASSERT(mkdtemp(tmp) != NULL);
	snprintf(global, sizeof(global), "%s/global", tmp);
	snprintf(path, sizeof(path), "%s/pg_control", global);
	UT_ASSERT_EQ(mkdir(global, 0700), 0);
	fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, &image, sizeof(image)), sizeof(image));
	UT_ASSERT_EQ(close(fd), 0);
	DataDir = tmp;
	cluster_shared_config = true;
	binding_result = PGRAC_CONTROL_BINDING_MISSING;
	prepare_calls = legacy_calls = open_calls = write_opens = 0;
	reset_error();
	caught = sigsetjmp(boundary, 1);
	if (!caught)
		LocalProcessControlFile(false);
	UT_ASSERT_EQ(caught, 1);
	UT_ASSERT_EQ(error_code, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT_EQ(prepare_calls, 0);
	UT_ASSERT_EQ(legacy_calls, 0);
	UT_ASSERT_EQ(open_calls, 1);
	UT_ASSERT_EQ(write_opens, 0);
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(read(fd, &after, sizeof(after)), sizeof(after));
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT(memcmp(&image, &after, sizeof(image)) == 0);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(rmdir(global), 0);
	UT_ASSERT_EQ(rmdir(tmp), 0);
	DataDir = NULL;
}
UT_TEST(test_nonshared_control_keeps_native_checksum_choice)
{
	cluster_shared_config = false;
	cluster_shared_catalog = false;
	for (uint32 version = 0; version <= PG_DATA_CHECKSUM_VERSION; version++) {
		ControlFileData image = checksum_control(version, DB_SHUTDOWNED);
		int caught;
		reset_error();
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			XLogValidateControlFile(&image);
		UT_ASSERT_EQ(caught, 0);
	}
}

UT_TEST(test_create_concurrently_refuses_before_native_mutation)
{
	IndexStmt stmt = { 0 };
	int caught;
	stmt.type = T_IndexStmt;
	for (int mode = 0; mode < 3; ++mode)
		for (int concurrent = 0; concurrent <= 1; ++concurrent)
			for (int entry = 0; entry < 2; ++entry) {
				cluster_shared_config = mode == 1;
				cluster_shared_catalog = mode == 2;
				stmt.concurrent = concurrent;
				reset_error();
				caught = sigsetjmp(boundary, 1);
				if (!caught) {
					if (entry == 0)
						sql_entry((Node *)&stmt);
					else
						DefineIndex(1, &stmt, 0, 0, 0, -1, false, false, false, false, false);
				}
				UT_ASSERT_EQ(caught, mode != 0 && concurrent);
				if (caught)
					UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
			}
}

UT_TEST(test_reindex_kinds_and_false_option)
{
	ReindexStmt stmt = { 0 };
	DefElem option = { 0 };
	Integer boolean = { 0 };
	ListCell cell;
	List options = { 0 };
	int caught;
	stmt.type = T_ReindexStmt;
	option.type = T_DefElem;
	option.defname = "concurrently";
	boolean.type = T_Integer;
	cell.ptr_value = &option;
	options.type = T_List;
	options.length = options.max_length = 1;
	options.elements = &cell;
	stmt.params = &options;
	for (int mode = 0; mode < 3; ++mode)
		for (int value = 0; value < 3; ++value)
			for (int kind = REINDEX_OBJECT_INDEX; kind <= REINDEX_OBJECT_DATABASE; ++kind)
				for (int entry = 0; entry < 2; ++entry) {
					cluster_shared_config = mode == 1;
					cluster_shared_catalog = mode == 2;
					boolean.ival = value == 1;
					option.arg = value == 2 ? NULL : (Node *)&boolean;
					stmt.kind = kind;
					reset_error();
					caught = sigsetjmp(boundary, 1);
					if (!caught) {
						if (entry == 0)
							sql_entry((Node *)&stmt);
						else
							ExecReindex(NULL, &stmt, true);
					}
					UT_ASSERT_EQ(caught, mode != 0);
					if (caught)
						UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
				}
}

static void
one_element(List *list, ListCell *cell, void *value)
{
	memset(list, 0, sizeof(*list));
	list->type = T_List;
	list->length = list->max_length = 1;
	list->elements = cell;
	cell->ptr_value = value;
}

UT_TEST(test_shared_index_access_methods_refuse_before_utility_or_define)
{
	char *methods[] = { NULL, "btree", "hash", "gin", "gist", "spgist", "brin", "rtree", "custom" };
	IndexStmt stmt = { 0 };
	int caught;

	stmt.type = T_IndexStmt;
	for (int mode = 0; mode < 3; mode++)
		for (unsigned am = 0; am < lengthof(methods); am++)
			for (int entry = 0; entry < 2; entry++) {
				cluster_shared_config = mode == 1;
				cluster_shared_catalog = mode == 2;
				stmt.accessMethod = methods[am];
				reset_error();
				caught = sigsetjmp(boundary, 1);
				if (!caught) {
					if (entry == 0)
						sql_entry((Node *)&stmt);
					else
						DefineIndex(1, &stmt, 0, 0, 0, -1, false, false, false, false, false);
				}
				UT_ASSERT_EQ(caught, mode != 0 && am >= 2);
				if (caught) {
					UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
					UT_ASSERT(strstr(error_message, "btree") != NULL);
				}
			}
}

UT_TEST(test_nonbtree_constraints_rejected_before_create_or_alter_table)
{
	Constraint constraint = { 0 };
	ColumnDef column = { 0 };
	CreateStmt create = { 0 };
	AlterTableStmt alter = { 0 };
	AlterTableCmd command = { 0 };
	List constraints, tableelts, commands;
	ListCell constraint_cell, table_cell, command_cell;
	char *methods[] = { NULL, "btree", "hash", "gin", "gist", "spgist", "brin" };
	int caught;

	constraint.type = T_Constraint;
	column.type = T_ColumnDef;
	create.type = T_CreateStmt;
	alter.type = T_AlterTableStmt;
	command.type = T_AlterTableCmd;
	one_element(&constraints, &constraint_cell, &constraint);
	one_element(&commands, &command_cell, &command);
	column.constraints = &constraints;
	alter.cmds = &commands;
	for (int mode = 0; mode < 3; mode++)
		for (unsigned am = 0; am < lengthof(methods); am++)
			for (int shape = 0; shape < 5; shape++) {
				Node *stmt;

				cluster_shared_config = mode == 1;
				cluster_shared_catalog = mode == 2;
				constraint.contype = CONSTR_EXCLUSION;
				constraint.access_method = methods[am];
				create.constraints = NIL;
				create.tableElts = NIL;
				if (shape <= 2) {
					one_element(&tableelts, &table_cell,
								shape == 0 ? (void *)&column : (void *)&constraint);
					if (shape == 2)
						create.constraints = &constraints;
					else
						create.tableElts = &tableelts;
					stmt = (Node *)&create;
				} else {
					command.subtype = shape == 3 ? AT_AddConstraint : AT_AddColumn;
					command.def = shape == 3 ? (Node *)&constraint : (Node *)&column;
					stmt = (Node *)&alter;
				}
				reset_error();
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					sql_entry(stmt);
				UT_ASSERT_EQ(caught, mode != 0 && am >= 2);
				if (caught)
					UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
			}
}

UT_TEST(test_schema_checks_nested_index_before_namespace_creation)
{
	CreateSchemaStmt schema = { 0 };
	IndexStmt index = { 0 };
	List children;
	ListCell child;
	int caught;

	schema.type = T_CreateSchemaStmt;
	index.type = T_IndexStmt;
	index.accessMethod = "gin";
	one_element(&children, &child, &index);
	schema.schemaElts = &children;
	cluster_shared_config = true;
	cluster_shared_catalog = false;
	reset_error();
	caught = sigsetjmp(boundary, 1);
	if (!caught)
		sql_entry((Node *)&schema);
	UT_ASSERT_EQ(caught, 1);
	UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
}

UT_TEST(test_native_index_create_requires_builtin_btree_oid)
{
	Oid methods[] = { BTREE_AM_OID,	 HASH_AM_OID, GIN_AM_OID, GIST_AM_OID,
					  SPGIST_AM_OID, BRIN_AM_OID, 99999,	  InvalidOid };
	RelationData relation = { 0 };
	int caught;

	for (int mode = 0; mode < 3; mode++)
		for (unsigned am = 0; am < lengthof(methods); am++) {
			cluster_shared_config = mode == 1;
			cluster_shared_catalog = mode == 2;
			reset_error();
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				index_create(&relation, "test", 0, 0, 0, 0, NULL, NIL, methods[am], 0, NULL, NULL,
							 NULL, 0, 0, 0, false, false, NULL);
			UT_ASSERT_EQ(caught, mode != 0 && am != 0);
			if (caught)
				UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		}
}

UT_TEST(test_primary_unique_and_nonindex_constraints_keep_native_admission)
{
	Constraint constraint = { 0 };
	ConstrType kinds[] = { CONSTR_PRIMARY, CONSTR_UNIQUE,  CONSTR_EXCLUSION,
						   CONSTR_CHECK,   CONSTR_DEFAULT, CONSTR_FOREIGN };
	int caught;

	constraint.type = T_Constraint;
	cluster_shared_config = true;
	cluster_shared_catalog = false;
	for (unsigned i = 0; i < lengthof(kinds); i++) {
		constraint.contype = kinds[i];
		constraint.access_method = i < 3 ? "btree" : "ignored_nonindex_field";
		reset_error();
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			sql_entry((Node *)&constraint);
		UT_ASSERT_EQ(caught, 0);
	}
}

static void
check_utility_scope(Node *stmt, bool refused)
{
	int mode;

	for (mode = 0; mode < 3; mode++) {
		int caught;

		cluster_shared_config = mode == 1;
		cluster_shared_catalog = mode == 2;
		reset_error();
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			sql_entry(stmt);
		UT_ASSERT_EQ(caught, mode != 0 && refused);
		if (caught)
			UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	}
}

UT_TEST(test_shared_removed_commands_refuse_at_native_utility_entry)
{
	NodeTag refused[] = { T_ClusterStmt,		  T_CreatedbStmt,		  T_DropdbStmt,
						  T_CreateTableSpaceStmt, T_DropTableSpaceStmt,	  T_CreateExtensionStmt,
						  T_RefreshMatViewStmt,	  T_AlterTableMoveAllStmt };
	VacuumStmt vacuum = { 0 };
	DefElem full = { 0 };
	Integer boolean = { 0 };
	List options;
	ListCell opt;
	unsigned i;

	for (i = 0; i < lengthof(refused); i++) {
		Node stmt = { .type = refused[i] };
		check_utility_scope(&stmt, true);
	}
	vacuum.type = T_VacuumStmt;
	vacuum.is_vacuumcmd = true;
	check_utility_scope((Node *)&vacuum, false);
	full.type = T_DefElem;
	full.defname = "full";
	boolean.type = T_Integer;
	boolean.ival = 0;
	full.arg = (Node *)&boolean;
	one_element(&options, &opt, &full);
	vacuum.options = &options;
	check_utility_scope((Node *)&vacuum, false);
	boolean.ival = 1;
	check_utility_scope((Node *)&vacuum, true);
	full.arg = NULL;
	check_utility_scope((Node *)&vacuum, true);
}

UT_TEST(test_unlogged_and_matview_create_and_alter_refuse_before_work)
{
	RangeVar target = { 0 };
	CreateStmt create = { 0 };
	CreateTableAsStmt ctas = { 0 };
	IntoClause into = { 0 };
	AlterTableStmt alter = { 0 };
	AlterTableCmd command = { 0 };
	DropStmt drop = { 0 };
	RenameStmt rename = { 0 };
	AlterObjectSchemaStmt schema = { 0 };
	List cmds;
	ListCell cmd;

	target.type = T_RangeVar;
	target.relpersistence = RELPERSISTENCE_UNLOGGED;
	create.type = T_CreateStmt;
	create.relation = &target;
	check_utility_scope((Node *)&create, true);
	ctas.type = T_CreateTableAsStmt;
	ctas.objtype = OBJECT_TABLE;
	ctas.into = &into;
	into.type = T_IntoClause;
	into.rel = &target;
	check_utility_scope((Node *)&ctas, true);
	target.relpersistence = RELPERSISTENCE_PERMANENT;
	check_utility_scope((Node *)&create, false);
	check_utility_scope((Node *)&ctas, false);
	ctas.objtype = OBJECT_MATVIEW;
	check_utility_scope((Node *)&ctas, true);
	alter.type = T_AlterTableStmt;
	alter.objtype = OBJECT_TABLE;
	command.type = T_AlterTableCmd;
	command.subtype = AT_SetUnLogged;
	one_element(&cmds, &cmd, &command);
	alter.cmds = &cmds;
	check_utility_scope((Node *)&alter, true);
	command.subtype = AT_SetTableSpace;
	check_utility_scope((Node *)&alter, true);
	command.subtype = AT_SetStatistics;
	check_utility_scope((Node *)&alter, false);
	alter.objtype = OBJECT_MATVIEW;
	check_utility_scope((Node *)&alter, true);
	drop.type = T_DropStmt;
	drop.removeType = OBJECT_MATVIEW;
	check_utility_scope((Node *)&drop, true);
	drop.removeType = OBJECT_INDEX;
	check_utility_scope((Node *)&drop, false);
	rename.type = T_RenameStmt;
	rename.renameType = OBJECT_MATVIEW;
	check_utility_scope((Node *)&rename, true);
	schema.type = T_AlterObjectSchemaStmt;
	schema.objectType = OBJECT_MATVIEW;
	check_utility_scope((Node *)&schema, true);
}

UT_TEST(test_schema_prechecks_removed_children_but_retains_supported_commands)
{
	CreateSchemaStmt schema = { 0 };
	CreateStmt create = { 0 };
	RangeVar target = { 0 };
	List children;
	ListCell child;
	Node trunc = { .type = T_TruncateStmt };
	Node view = { .type = T_ViewStmt };

	schema.type = T_CreateSchemaStmt;
	create.type = T_CreateStmt;
	create.relation = &target;
	target.type = T_RangeVar;
	target.relpersistence = RELPERSISTENCE_UNLOGGED;
	one_element(&children, &child, &create);
	schema.schemaElts = &children;
	check_utility_scope((Node *)&schema, true);
	target.relpersistence = RELPERSISTENCE_PERMANENT;
	check_utility_scope((Node *)&schema, false);
	check_utility_scope(&trunc, false);
	check_utility_scope(&view, false);
}

UT_TEST(test_shared_two_phase_refuses_but_transaction_control_remains_native)
{
	TransactionStmt stmt = { .type = T_TransactionStmt };
	TransactionStmtKind kinds[] = { TRANS_STMT_BEGIN,			TRANS_STMT_START,
									TRANS_STMT_COMMIT,			TRANS_STMT_ROLLBACK,
									TRANS_STMT_SAVEPOINT,		TRANS_STMT_RELEASE,
									TRANS_STMT_ROLLBACK_TO,		TRANS_STMT_PREPARE,
									TRANS_STMT_COMMIT_PREPARED, TRANS_STMT_ROLLBACK_PREPARED };

	for (unsigned i = 0; i < lengthof(kinds); i++) {
		stmt.kind = kinds[i];
		check_utility_scope((Node *)&stmt, i >= 7);
	}
}

UT_TEST(test_rdma_drop_checks_native_targets_before_later_utility_work)
{
	ObjectType kinds[] = { OBJECT_TABLE, OBJECT_INDEX, OBJECT_SEQUENCE, OBJECT_SCHEMA };
	cluster_shared_config = true;
	for (int tier = CLUSTER_IC_TIER_2; tier <= CLUSTER_IC_TIER_3; tier++) {
		cluster_interconnect_tier = tier;
		for (unsigned i = 0; i < lengthof(kinds); i++) {
			DropStmt drop = { .type = T_DropStmt, .removeType = kinds[i] };
			int caught;
			storage_drop_preflights = utility_after_preflight = 0;
			drop_target_shared = true;
			reset_error();
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				sql_entry((Node *)&drop);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
			UT_ASSERT_EQ(storage_drop_preflights, 1);
			UT_ASSERT_EQ(utility_after_preflight, 0);
			drop_target_shared = false;
			sql_entry((Node *)&drop);
			UT_ASSERT_EQ(storage_drop_preflights, 2);
			UT_ASSERT_EQ(utility_after_preflight, 1);
		}
	}
	{
		DropOwnedStmt owned = { .type = T_DropOwnedStmt };
		int caught;
		storage_drop_preflights = utility_after_preflight = 0;
		drop_target_shared = true;
		reset_error();
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			sql_entry((Node *)&owned);
		UT_ASSERT_EQ(caught, 1);
		UT_ASSERT_EQ(storage_drop_preflights, 1);
		UT_ASSERT_EQ(utility_after_preflight, 0);
	}
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
}

UT_TEST(test_shared_smgr_drop_preflight_is_independent_of_shared_catalog_gucs)
{
	DropStmt drop = {0};
	DropOwnedStmt owned = {0};
	Node *commands[] = {(Node *)&drop, (Node *)&drop, (Node *)&owned};
	int caught;

	drop.type = T_DropStmt;
	owned.type = T_DropOwnedStmt;
	cluster_shared_config = cluster_shared_catalog = false;
	utility_storage_mode = utility_in_transaction = true;
	for (unsigned i = 0; i < lengthof(commands); i++) {
		drop.removeType = i == 0 ? OBJECT_TABLE : OBJECT_SCHEMA;
		storage_drop_preflights = utility_after_preflight = 0;
		drop_target_shared = true;
		reset_error();
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			sql_entry(commands[i]);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(error_code, ERRCODE_ACTIVE_SQL_TRANSACTION);
		UT_ASSERT_EQ(storage_drop_preflights, 1);
		UT_ASSERT_EQ(utility_after_preflight, 0);
		drop_target_shared = false;
		sql_entry(commands[i]);
		UT_ASSERT_EQ(storage_drop_preflights, 2);
		UT_ASSERT_EQ(utility_after_preflight, 1);
	}
	utility_in_transaction = false;
	utility_storage_mode = true;
}

/* Actual utility guards/classification/prologue run above. Transaction identity,
 * canonical publication and later DDL/trigger work are explicit boundaries;
 * this checks entry ordering, not BIND durability or a live CREATE operation. */
static void
reset_create_preflight(void)
{
	cluster_shared_catalog = true;
	cluster_shared_config = true;
	utility_storage_mode = true;
	utility_in_child = utility_parallel = XactReadOnly = utility_expect_binding = false;
	utility_bind_ok = true;
	utility_top_xid = utility_current_xid = InvalidTransactionId;
	utility_xids = utility_binds = utility_prepares = utility_unbound = 0;
	utility_after_preflight = 0;
	reset_error();
}

UT_TEST(test_shared_create_binds_before_later_utility_work_and_reuses_top)
{
	CreateStmt create = { .type = T_CreateStmt };
	CreateTableAsStmt ctas = { .type = T_CreateTableAsStmt, .objtype = OBJECT_TABLE };
	Node *statements[] = { (Node *)&create, (Node *)&ctas };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		reset_create_preflight();
		utility_expect_binding = true;
		create_sql_entry(statements[i]);
		UT_ASSERT_EQ(utility_unbound, 0);
		UT_ASSERT_EQ(utility_after_preflight, 1);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_prepares, 1);
		UT_ASSERT_EQ(utility_binds, 1);
		create_sql_entry(statements[i]);
		UT_ASSERT_EQ(utility_unbound, 0);
		UT_ASSERT_EQ(utility_after_preflight, 2);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_prepares, 2);
		UT_ASSERT_EQ(utility_binds, 1);
	}
}

UT_TEST(test_create_publication_refusal_precedes_later_utility_work)
{
	CreateStmt create = { .type = T_CreateStmt };
	CreateTableAsStmt ctas = { .type = T_CreateTableAsStmt, .objtype = OBJECT_TABLE };
	Node *statements[] = { (Node *)&create, (Node *)&ctas };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		int caught;

		reset_create_preflight();
		utility_bind_ok = false;
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			create_sql_entry(statements[i]);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(error_code, ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_prepares, 1);
		UT_ASSERT_EQ(utility_binds, 0);
		UT_ASSERT_EQ(utility_after_preflight, 0);
	}
}

UT_TEST(test_create_rejects_child_or_wrong_top_before_xid_and_binding)
{
	CreateStmt create = { .type = T_CreateStmt };

	for (unsigned child = 0; child < 2; child++) {
		int caught;

		reset_create_preflight();
		utility_in_child = child != 0;
		if (!child) {
			utility_top_xid = 400;
			utility_current_xid = 401;
		}
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			create_sql_entry((Node *)&create);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(error_code, child ? ERRCODE_FEATURE_NOT_SUPPORTED : ERRCODE_INTERNAL_ERROR);
		UT_ASSERT_EQ(utility_xids, 0);
		UT_ASSERT_EQ(utility_prepares, 0);
		UT_ASSERT_EQ(utility_after_preflight, 0);
	}
}

UT_TEST(test_create_preflight_only_targets_shared_catalog_storage)
{
	CreateStmt create = { .type = T_CreateStmt };

	for (unsigned mode = 0; mode < 4; mode++) {
		reset_create_preflight();
		cluster_shared_catalog = (mode & 1) != 0;
		cluster_shared_config = false;
		utility_storage_mode = (mode & 2) != 0;
		create_sql_entry((Node *)&create);
		UT_ASSERT_EQ(utility_after_preflight, 1);
		UT_ASSERT_EQ(utility_xids, mode == 3 ? 1 : 0);
		UT_ASSERT_EQ(utility_prepares, mode == 3 ? 1 : 0);
		UT_ASSERT_EQ(utility_binds, mode == 3 ? 1 : 0);
	}
}

UT_TEST(test_create_original_readonly_and_parallel_guards_precede_binding)
{
	CreateStmt create = { .type = T_CreateStmt };

	for (unsigned parallel = 0; parallel < 2; parallel++) {
		int caught;

		reset_create_preflight();
		XactReadOnly = parallel == 0;
		utility_parallel = parallel != 0;
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			create_sql_entry((Node *)&create);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(error_code, parallel ? ERRCODE_INVALID_TRANSACTION_STATE
										  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
		UT_ASSERT_EQ(utility_xids, 0);
		UT_ASSERT_EQ(utility_prepares, 0);
		UT_ASSERT_EQ(utility_after_preflight, 0);
	}
}

UT_TEST(test_unsupported_creates_refuse_before_xid_or_binding)
{
	RangeVar target = { .type = T_RangeVar, .relpersistence = RELPERSISTENCE_UNLOGGED };
	CreateStmt create = { .type = T_CreateStmt, .relation = &target };
	CreateTableAsStmt ctas = { .type = T_CreateTableAsStmt, .objtype = OBJECT_MATVIEW };
	DefineStmt type = { .type = T_DefineStmt, .kind = OBJECT_TYPE };
	DefineStmt operator= {.type = T_DefineStmt, .kind = OBJECT_OPERATOR };
	/* Grammar routes CREATE LANGUAGE without HANDLER through CREATE EXTENSION. */
	CreateExtensionStmt language_extension
		= { .type = T_CreateExtensionStmt, .extname = "target_language" };
	CreateExtensionStmt replace_language_extension
		= { .type = T_CreateExtensionStmt, .extname = "target_language", .if_not_exists = true };
	Node *statements[]
		= { (Node *)&create, (Node *)&ctas, (Node *)&type,
			(Node *)&operator,(Node *) & language_extension, (Node *)&replace_language_extension };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		int caught;

		reset_create_preflight();
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			create_sql_entry(statements[i]);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(utility_xids, 0);
		UT_ASSERT_EQ(utility_prepares, 0);
		UT_ASSERT_EQ(utility_after_preflight, 0);
	}
}

UT_TEST(test_noncreate_utility_does_not_reserve_canonical_binding)
{
	VariableShowStmt show = { .type = T_VariableShowStmt };
	TransactionStmt transaction = { .type = T_TransactionStmt, .kind = TRANS_STMT_BEGIN };
	VacuumStmt vacuum = { .type = T_VacuumStmt };
	CopyStmt copy = { .type = T_CopyStmt };
	Node *statements[] = { (Node *)&show, (Node *)&transaction, (Node *)&vacuum, (Node *)&copy };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		reset_create_preflight();
		create_sql_entry(statements[i]);
		UT_ASSERT_EQ(utility_after_preflight, 1);
		UT_ASSERT_EQ(utility_xids, 0);
		UT_ASSERT_EQ(utility_prepares, 0);
	}
}

/* The same verbatim utility prologue must finish BIND before TRUNCATE can
 * reach its DDL locks/triggers. Later native DDL remains an explicit boundary. */
UT_TEST(test_shared_truncate_variants_and_targets_bind_before_later_work)
{
	RangeVar targets[2] = { { .type = T_RangeVar }, { .type = T_RangeVar } };
	ListCell cells[2] = { { .ptr_value = &targets[0] }, { .ptr_value = &targets[1] } };
	List relations = { .type = T_List, .length = 2, .max_length = 2, .elements = cells };
	TruncateStmt truncate = { .type = T_TruncateStmt, .relations = &relations };

	for (unsigned variant = 0; variant < 4; variant++) {
		reset_create_preflight();
		utility_expect_binding = true;
		truncate.restart_seqs = (variant & 1) != 0;
		truncate.behavior = variant & 2 ? DROP_CASCADE : DROP_RESTRICT;
		create_sql_entry((Node *)&truncate);
		UT_ASSERT_EQ(utility_unbound, 0);
		UT_ASSERT_EQ(utility_after_preflight, 1);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_prepares, 1);
		UT_ASSERT_EQ(utility_binds, 1);
	}
}

UT_TEST(test_truncate_reuses_existing_top_binding_across_utility_calls)
{
	CreateStmt create = { .type = T_CreateStmt };
	TruncateStmt truncate = { .type = T_TruncateStmt };

	reset_create_preflight();
	utility_expect_binding = true;
	create_sql_entry((Node *)&create);
	create_sql_entry((Node *)&truncate);
	create_sql_entry((Node *)&truncate);
	UT_ASSERT_EQ(utility_unbound, 0);
	UT_ASSERT_EQ(utility_after_preflight, 3);
	UT_ASSERT_EQ(utility_xids, 1);
	UT_ASSERT_EQ(utility_prepares, 3);
	UT_ASSERT_EQ(utility_binds, 1);
}

UT_TEST(test_truncate_publication_refusal_precedes_later_utility_work)
{
	TruncateStmt truncate = { .type = T_TruncateStmt };
	int caught;

	reset_create_preflight();
	utility_bind_ok = false;
	caught = sigsetjmp(boundary, 1);
	if (!caught)
		create_sql_entry((Node *)&truncate);
	UT_ASSERT(caught);
	UT_ASSERT_EQ(error_code, ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(utility_xids, 1);
	UT_ASSERT_EQ(utility_prepares, 1);
	UT_ASSERT_EQ(utility_binds, 0);
	UT_ASSERT_EQ(utility_after_preflight, 0);
}

UT_TEST(test_truncate_child_or_wrong_top_refuses_before_allocation)
{
	TruncateStmt truncate = { .type = T_TruncateStmt };

	for (unsigned child = 0; child < 2; child++) {
		int caught;

		reset_create_preflight();
		utility_in_child = child != 0;
		if (!child) {
			utility_top_xid = 400;
			utility_current_xid = 401;
		}
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			create_sql_entry((Node *)&truncate);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(error_code, child ? ERRCODE_FEATURE_NOT_SUPPORTED : ERRCODE_INTERNAL_ERROR);
		UT_ASSERT_EQ(utility_xids, 0);
		UT_ASSERT_EQ(utility_prepares, 0);
		UT_ASSERT_EQ(utility_after_preflight, 0);
	}
}

UT_TEST(test_truncate_preflight_requires_shared_catalog_storage)
{
	TruncateStmt truncate = { .type = T_TruncateStmt };

	for (unsigned mode = 0; mode < 8; mode++) {
		reset_create_preflight();
		cluster_shared_catalog = (mode & 1) != 0;
		utility_storage_mode = (mode & 2) != 0;
		cluster_shared_config = (mode & 4) != 0;
		create_sql_entry((Node *)&truncate);
		UT_ASSERT_EQ(utility_after_preflight, 1);
		UT_ASSERT_EQ(utility_xids, (mode & 3) == 3);
		UT_ASSERT_EQ(utility_prepares, (mode & 3) == 3);
		UT_ASSERT_EQ(utility_binds, (mode & 3) == 3);
	}
}

UT_TEST(test_truncate_original_readonly_parallel_guards_precede_binding)
{
	TruncateStmt truncate = { .type = T_TruncateStmt };

	for (unsigned parallel = 0; parallel < 2; parallel++) {
		int caught;

		reset_create_preflight();
		XactReadOnly = parallel == 0;
		utility_parallel = parallel != 0;
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			create_sql_entry((Node *)&truncate);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(error_code, parallel ? ERRCODE_INVALID_TRANSACTION_STATE
										  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
		UT_ASSERT_EQ(utility_xids, 0);
		UT_ASSERT_EQ(utility_prepares, 0);
		UT_ASSERT_EQ(utility_after_preflight, 0);
	}
}

UT_TEST(test_unsupported_rename_or_schema_move_refuses_before_binding)
{
	const ObjectType types[] = { OBJECT_MATVIEW, OBJECT_TYPE,	  OBJECT_DOMAIN,   OBJECT_OPERATOR,
								 OBJECT_OPCLASS, OBJECT_OPFAMILY, OBJECT_EXTENSION };
	RenameStmt rename = { .type = T_RenameStmt, .relationType = OBJECT_TABLE };
	AlterObjectSchemaStmt move = { .type = T_AlterObjectSchemaStmt };
	Node *statements[] = { (Node *)&rename, (Node *)&move };
	const RenameStmt variants[]
		= { { .type = T_RenameStmt, .renameType = OBJECT_COLUMN, .relationType = OBJECT_MATVIEW },
			{ .type = T_RenameStmt, .renameType = OBJECT_ATTRIBUTE, .relationType = OBJECT_TYPE },
			{ .type = T_RenameStmt, .renameType = OBJECT_DOMCONSTRAINT } };

	for (unsigned i = 0; i < lengthof(types); i++) {
		rename.renameType = move.objectType = types[i];
		for (unsigned j = 0; j < lengthof(statements); j++) {
			int caught;

			reset_create_preflight();
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				create_sql_entry(statements[j]);
			UT_ASSERT(caught);
			UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_binds, 0);
			UT_ASSERT_EQ(utility_after_preflight, 0);
		}
	}
	/* The original support check also classifies the associated relation,
	 * and a domain constraint is part of the unsupported type family. */
	for (unsigned variant = 0; variant < lengthof(variants); variant++) {
		int caught;

		rename = variants[variant];
		reset_create_preflight();
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			create_sql_entry((Node *)&rename);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(utility_xids, 0);
		UT_ASSERT_EQ(utility_prepares, 0);
		UT_ASSERT_EQ(utility_binds, 0);
		UT_ASSERT_EQ(utility_after_preflight, 0);
	}
}

UT_TEST(test_catalog_writes_bind_before_later_work_and_reuse_top)
{
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt };
	CreateSeqStmt sequence = { .type = T_CreateSeqStmt };
	IndexStmt index = { .type = T_IndexStmt };
	ViewStmt view = { .type = T_ViewStmt };
	RenameStmt rename
		= { .type = T_RenameStmt, .renameType = OBJECT_TABLE, .relationType = OBJECT_TABLE };
	AlterObjectSchemaStmt move = { .type = T_AlterObjectSchemaStmt, .objectType = OBJECT_TABLE };
	GrantStmt grant = { .type = T_GrantStmt,
						.is_grant = true,
						.targtype = ACL_TARGET_OBJECT,
						.objtype = OBJECT_TABLE };
	GrantRoleStmt role = { .type = T_GrantRoleStmt, .is_grant = true };
	GrantStmt default_action = { .type = T_GrantStmt,
								 .is_grant = true,
								 .targtype = ACL_TARGET_DEFAULTS,
								 .objtype = OBJECT_TABLE };
	AlterDefaultPrivilegesStmt defaults
		= { .type = T_AlterDefaultPrivilegesStmt, .action = &default_action };
	CreateFunctionStmt create_function = { .type = T_CreateFunctionStmt };
	CreateFunctionStmt create_procedure = { .type = T_CreateFunctionStmt, .is_procedure = true };
	AlterFunctionStmt alter_function = { .type = T_AlterFunctionStmt, .objtype = OBJECT_FUNCTION };
	AlterFunctionStmt alter_procedure
		= { .type = T_AlterFunctionStmt, .objtype = OBJECT_PROCEDURE };
	AlterFunctionStmt alter_routine = { .type = T_AlterFunctionStmt, .objtype = OBJECT_ROUTINE };
	CreateTrigStmt trigger = { .type = T_CreateTrigStmt };
	RuleStmt rule = { .type = T_RuleStmt, .event = CMD_UPDATE };
	CreatePolicyStmt policy = { .type = T_CreatePolicyStmt, .permissive = true };
	AlterPolicyStmt alter_policy = { .type = T_AlterPolicyStmt };
	CommentStmt comment = { .type = T_CommentStmt, .objtype = OBJECT_TABLE };
	AlterStatsStmt statistics = { .type = T_AlterStatsStmt, .stxstattarget = 100 };
	CreateEventTrigStmt create_event
		= { .type = T_CreateEventTrigStmt, .eventname = "ddl_command_start" };
	AlterEventTrigStmt alter_event = { .type = T_AlterEventTrigStmt, .tgenabled = 'O' };
	AlterCollationStmt alter_collation = { .type = T_AlterCollationStmt };
	DefineStmt aggregate = { .type = T_DefineStmt, .kind = OBJECT_AGGREGATE };
	DefineStmt collation = { .type = T_DefineStmt, .kind = OBJECT_COLLATION };
	DefineStmt parser = { .type = T_DefineStmt, .kind = OBJECT_TSPARSER };
	DefineStmt dictionary = { .type = T_DefineStmt, .kind = OBJECT_TSDICTIONARY };
	DefineStmt template = { .type = T_DefineStmt, .kind = OBJECT_TSTEMPLATE };
	DefineStmt configuration = { .type = T_DefineStmt, .kind = OBJECT_TSCONFIGURATION };
	RangeVar stats_relation = { .type = T_RangeVar, .relname = "target" };
	ListCell stats_cell = { .ptr_value = &stats_relation };
	List stats_relations
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &stats_cell };
	CreateStatsStmt create_stats = { .type = T_CreateStatsStmt, .relations = &stats_relations };
	AlterSeqStmt alter_sequence = { .type = T_AlterSeqStmt };
	AlterTSDictionaryStmt alter_dictionary = { .type = T_AlterTSDictionaryStmt };
	AlterTSConfigurationStmt mapping_add
		= { .type = T_AlterTSConfigurationStmt, .kind = ALTER_TSCONFIG_ADD_MAPPING };
	AlterTSConfigurationStmt mapping_alter = { .type = T_AlterTSConfigurationStmt,
											   .kind = ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN,
											   .override = true };
	AlterTSConfigurationStmt mapping_replace = { .type = T_AlterTSConfigurationStmt,
												 .kind = ALTER_TSCONFIG_REPLACE_DICT,
												 .replace = true };
	AlterTSConfigurationStmt mapping_replace_token
		= { .type = T_AlterTSConfigurationStmt,
			.kind = ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN,
			.replace = true };
	AlterTSConfigurationStmt mapping_drop
		= { .type = T_AlterTSConfigurationStmt, .kind = ALTER_TSCONFIG_DROP_MAPPING };
	RoleSpec role_target
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "target" };
	CreateRoleStmt create_role
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_ROLE, .role = "target" };
	CreateRoleStmt create_user
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_USER, .role = "target_user" };
	CreateRoleStmt create_group
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_GROUP, .role = "target_group" };
	AlterRoleStmt alter_role = { .type = T_AlterRoleStmt, .role = &role_target, .action = 1 };
	VariableSetStmt role_setting
		= { .type = T_VariableSetStmt, .kind = VAR_RESET, .name = "application_name" };
	AlterRoleSetStmt alter_settings
		= { .type = T_AlterRoleSetStmt, .role = &role_target, .setstmt = &role_setting };
	AlterRoleSetStmt all_settings
		= { .type = T_AlterRoleSetStmt, .database = "target_database", .setstmt = &role_setting };
	ListCell role_cell = { .ptr_value = &role_target };
	List roles = { .type = T_List, .length = 1, .max_length = 1, .elements = &role_cell };
	DropRoleStmt drop_role = { .type = T_DropRoleStmt, .roles = &roles };
	VariableSetStmt database_setting
		= { .type = T_VariableSetStmt, .kind = VAR_RESET, .name = "application_name" };
	AlterDatabaseSetStmt database_settings = { .type = T_AlterDatabaseSetStmt,
											   .dbname = "target_database",
											   .setstmt = &database_setting };
	AlterDatabaseRefreshCollStmt database_collation
		= { .type = T_AlterDatabaseRefreshCollStmt, .dbname = "target_database" };
	String definition_name = { .type = T_String, .sval = "target_handler" };
	ListCell definition_cell = { .ptr_value = &definition_name };
	List definition_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &definition_cell };
	CreatePLangStmt language = { .type = T_CreatePLangStmt,
								 .plname = "target_language",
								 .plhandler = &definition_names };
	CreateConversionStmt conversion = { .type = T_CreateConversionStmt,
										.conversion_name = &definition_names,
										.for_encoding_name = "UTF8",
										.to_encoding_name = "LATIN1",
										.func_name = &definition_names };
	TypeName source_type = { .type = T_TypeName, .typeOid = INT4OID, .typemod = -1 };
	TypeName target_type = { .type = T_TypeName, .typeOid = TEXTOID, .typemod = -1 };
	CreateCastStmt cast = { .type = T_CreateCastStmt,
							.sourcetype = &source_type,
							.targettype = &target_type,
							.context = COERCION_EXPLICIT,
							.inout = true };
	ObjectWithArgs transform_function = { .type = T_ObjectWithArgs, .objname = &definition_names };
	CreateTransformStmt transform = { .type = T_CreateTransformStmt,
									  .type_name = &source_type,
									  .lang = "target_language",
									  .fromsql = &transform_function };
	CreateAmStmt access_method = { .type = T_CreateAmStmt,
								   .amname = "target_am",
								   .handler_name = &definition_names,
								   .amtype = AMTYPE_INDEX };
	Node *statements[] = { (Node *)&schema,
						   (Node *)&sequence,
						   (Node *)&index,
						   (Node *)&view,
						   (Node *)&rename,
						   (Node *)&move,
						   (Node *)&grant,
						   (Node *)&role,
						   (Node *)&defaults,
						   (Node *)&create_function,
						   (Node *)&create_procedure,
						   (Node *)&alter_function,
						   (Node *)&alter_procedure,
						   (Node *)&alter_routine,
						   (Node *)&trigger,
						   (Node *)&rule,
						   (Node *)&policy,
						   (Node *)&alter_policy,
						   (Node *)&comment,
						   (Node *)&statistics,
						   (Node *)&create_event,
						   (Node *)&alter_event,
						   (Node *)&alter_collation,
						   (Node *)&aggregate,
						   (Node *)&collation,
						   (Node *)&parser,
						   (Node *)&dictionary,
						   (Node *)&template,
						   (Node *)&configuration,
						   (Node *)&create_stats,
						   (Node *)&alter_sequence,
						   (Node *)&alter_dictionary,
						   (Node *)&mapping_add,
						   (Node *)&mapping_alter,
						   (Node *)&mapping_replace,
						   (Node *)&mapping_replace_token,
						   (Node *)&mapping_drop,
						   (Node *)&create_role,
						   (Node *)&create_user,
						   (Node *)&create_group,
						   (Node *)&alter_role,
						   (Node *)&alter_settings,
						   (Node *)&all_settings,
						   (Node *)&drop_role,
						   (Node *)&database_settings,
						   (Node *)&database_collation,
						   (Node *)&language,
						   (Node *)&conversion,
						   (Node *)&cast,
						   (Node *)&transform,
						   (Node *)&access_method };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		for (unsigned skip = 0; skip < 2; skip++) {
			reset_create_preflight();
			utility_expect_binding = true;
			schema.if_not_exists = sequence.if_not_exists = skip != 0;
			index.if_not_exists = index.unique = view.replace = skip != 0;
			rename.missing_ok = move.missing_ok = skip != 0;
			grant.is_grant = role.is_grant = default_action.is_grant = skip == 0;
			grant.targtype = skip ? ACL_TARGET_ALL_IN_SCHEMA : ACL_TARGET_OBJECT;
			create_function.replace = create_procedure.replace = skip != 0;
			trigger.replace = rule.replace = skip != 0;
			policy.permissive = skip == 0;
			comment.comment = skip ? NULL : "test comment";
			statistics.missing_ok = collation.if_not_exists = aggregate.replace = skip != 0;
			alter_event.tgenabled = skip ? 'D' : 'O';
			create_stats.if_not_exists = skip != 0;
			alter_sequence.missing_ok = mapping_drop.missing_ok = skip != 0;
			drop_role.missing_ok = skip != 0;
			role_setting.kind = skip ? VAR_RESET_ALL : VAR_RESET;
			role_setting.name = skip ? NULL : "application_name";
			database_setting.kind = skip ? VAR_RESET_ALL : VAR_RESET;
			database_setting.name = skip ? NULL : "application_name";
			language.replace = language.pltrusted = transform.replace = conversion.def = skip != 0;
			cast.context = skip ? COERCION_ASSIGNMENT : COERCION_EXPLICIT;
			access_method.amtype = skip ? AMTYPE_TABLE : AMTYPE_INDEX;
			create_sql_entry(statements[i]);
			UT_ASSERT_EQ(utility_unbound, 0);
			UT_ASSERT_EQ(utility_after_preflight, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			create_sql_entry(statements[i]);
			UT_ASSERT_EQ(utility_unbound, 0);
			UT_ASSERT_EQ(utility_after_preflight, 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_prepares, 2);
			UT_ASSERT_EQ(utility_binds, 1);
		}
	}
}

UT_TEST(test_schema_binds_before_catalog_children_and_children_reuse_top)
{
	CreateStmt table = { .type = T_CreateStmt };
	CreateSeqStmt sequence = { .type = T_CreateSeqStmt };
	IndexStmt index = { .type = T_IndexStmt };
	ViewStmt view = { .type = T_ViewStmt };
	GrantStmt grant = { .type = T_GrantStmt,
						.is_grant = true,
						.targtype = ACL_TARGET_OBJECT,
						.objtype = OBJECT_TABLE };
	CreateTrigStmt trigger = { .type = T_CreateTrigStmt };
	ListCell cells[6]
		= { { .ptr_value = &table }, { .ptr_value = &sequence }, { .ptr_value = &index },
			{ .ptr_value = &view },	 { .ptr_value = &grant },	 { .ptr_value = &trigger } };
	List children = { .type = T_List, .length = 6, .max_length = 6, .elements = cells };
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt, .schemaElts = &children };

	reset_create_preflight();
	utility_expect_binding = true;
	create_sql_entry((Node *)&schema);
	UT_ASSERT_EQ(utility_unbound, 0);
	UT_ASSERT_EQ(utility_after_preflight, 1);
	UT_ASSERT_EQ(utility_prepares, 1);
	/* Native schema-child dispatch is a boundary; each child runs the real
	 * utility prologue here, after the namespace would already exist. */
	create_sql_entry((Node *)&table);
	create_sql_entry((Node *)&sequence);
	create_sql_entry((Node *)&index);
	create_sql_entry((Node *)&view);
	create_sql_entry((Node *)&grant);
	create_sql_entry((Node *)&trigger);
	UT_ASSERT_EQ(utility_unbound, 0);
	UT_ASSERT_EQ(utility_after_preflight, 7);
	UT_ASSERT_EQ(utility_xids, 1);
	UT_ASSERT_EQ(utility_prepares, 7);
	UT_ASSERT_EQ(utility_binds, 1);
}

UT_TEST(test_catalog_writes_publication_refusal_precedes_later_work)
{
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt };
	CreateSeqStmt sequence = { .type = T_CreateSeqStmt };
	IndexStmt index = { .type = T_IndexStmt };
	ViewStmt view = { .type = T_ViewStmt };
	RenameStmt rename
		= { .type = T_RenameStmt, .renameType = OBJECT_TABLE, .relationType = OBJECT_TABLE };
	AlterObjectSchemaStmt move = { .type = T_AlterObjectSchemaStmt, .objectType = OBJECT_TABLE };
	GrantStmt grant = { .type = T_GrantStmt,
						.is_grant = true,
						.targtype = ACL_TARGET_OBJECT,
						.objtype = OBJECT_TABLE };
	GrantRoleStmt role = { .type = T_GrantRoleStmt, .is_grant = true };
	GrantStmt default_action = { .type = T_GrantStmt,
								 .is_grant = true,
								 .targtype = ACL_TARGET_DEFAULTS,
								 .objtype = OBJECT_TABLE };
	AlterDefaultPrivilegesStmt defaults
		= { .type = T_AlterDefaultPrivilegesStmt, .action = &default_action };
	CreateFunctionStmt create_function = { .type = T_CreateFunctionStmt };
	CreateFunctionStmt create_procedure = { .type = T_CreateFunctionStmt, .is_procedure = true };
	AlterFunctionStmt alter_function = { .type = T_AlterFunctionStmt, .objtype = OBJECT_FUNCTION };
	AlterFunctionStmt alter_procedure
		= { .type = T_AlterFunctionStmt, .objtype = OBJECT_PROCEDURE };
	AlterFunctionStmt alter_routine = { .type = T_AlterFunctionStmt, .objtype = OBJECT_ROUTINE };
	CreateTrigStmt trigger = { .type = T_CreateTrigStmt };
	RuleStmt rule = { .type = T_RuleStmt, .event = CMD_UPDATE };
	CreatePolicyStmt policy = { .type = T_CreatePolicyStmt, .permissive = true };
	AlterPolicyStmt alter_policy = { .type = T_AlterPolicyStmt };
	CommentStmt comment = { .type = T_CommentStmt, .objtype = OBJECT_TABLE };
	AlterStatsStmt statistics = { .type = T_AlterStatsStmt, .stxstattarget = 100 };
	CreateEventTrigStmt create_event
		= { .type = T_CreateEventTrigStmt, .eventname = "ddl_command_start" };
	AlterEventTrigStmt alter_event = { .type = T_AlterEventTrigStmt, .tgenabled = 'O' };
	AlterCollationStmt alter_collation = { .type = T_AlterCollationStmt };
	DefineStmt aggregate = { .type = T_DefineStmt, .kind = OBJECT_AGGREGATE };
	DefineStmt collation = { .type = T_DefineStmt, .kind = OBJECT_COLLATION };
	DefineStmt parser = { .type = T_DefineStmt, .kind = OBJECT_TSPARSER };
	DefineStmt dictionary = { .type = T_DefineStmt, .kind = OBJECT_TSDICTIONARY };
	DefineStmt template = { .type = T_DefineStmt, .kind = OBJECT_TSTEMPLATE };
	DefineStmt configuration = { .type = T_DefineStmt, .kind = OBJECT_TSCONFIGURATION };
	RangeVar stats_relation = { .type = T_RangeVar, .relname = "target" };
	ListCell stats_cell = { .ptr_value = &stats_relation };
	List stats_relations
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &stats_cell };
	CreateStatsStmt create_stats = { .type = T_CreateStatsStmt, .relations = &stats_relations };
	AlterSeqStmt alter_sequence = { .type = T_AlterSeqStmt };
	AlterTSDictionaryStmt alter_dictionary = { .type = T_AlterTSDictionaryStmt };
	AlterTSConfigurationStmt mapping_add
		= { .type = T_AlterTSConfigurationStmt, .kind = ALTER_TSCONFIG_ADD_MAPPING };
	AlterTSConfigurationStmt mapping_alter = { .type = T_AlterTSConfigurationStmt,
											   .kind = ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN,
											   .override = true };
	AlterTSConfigurationStmt mapping_replace = { .type = T_AlterTSConfigurationStmt,
												 .kind = ALTER_TSCONFIG_REPLACE_DICT,
												 .replace = true };
	AlterTSConfigurationStmt mapping_replace_token
		= { .type = T_AlterTSConfigurationStmt,
			.kind = ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN,
			.replace = true };
	AlterTSConfigurationStmt mapping_drop
		= { .type = T_AlterTSConfigurationStmt, .kind = ALTER_TSCONFIG_DROP_MAPPING };
	RoleSpec role_target
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "target" };
	CreateRoleStmt create_role
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_ROLE, .role = "target" };
	CreateRoleStmt create_user
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_USER, .role = "target_user" };
	CreateRoleStmt create_group
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_GROUP, .role = "target_group" };
	AlterRoleStmt alter_role = { .type = T_AlterRoleStmt, .role = &role_target, .action = 1 };
	VariableSetStmt role_setting
		= { .type = T_VariableSetStmt, .kind = VAR_RESET, .name = "application_name" };
	AlterRoleSetStmt alter_settings
		= { .type = T_AlterRoleSetStmt, .role = &role_target, .setstmt = &role_setting };
	AlterRoleSetStmt all_settings
		= { .type = T_AlterRoleSetStmt, .database = "target_database", .setstmt = &role_setting };
	ListCell role_cell = { .ptr_value = &role_target };
	List roles = { .type = T_List, .length = 1, .max_length = 1, .elements = &role_cell };
	DropRoleStmt drop_role = { .type = T_DropRoleStmt, .roles = &roles };
	VariableSetStmt database_setting
		= { .type = T_VariableSetStmt, .kind = VAR_RESET, .name = "application_name" };
	AlterDatabaseSetStmt database_settings = { .type = T_AlterDatabaseSetStmt,
											   .dbname = "target_database",
											   .setstmt = &database_setting };
	AlterDatabaseRefreshCollStmt database_collation
		= { .type = T_AlterDatabaseRefreshCollStmt, .dbname = "target_database" };
	String definition_name = { .type = T_String, .sval = "target_handler" };
	ListCell definition_cell = { .ptr_value = &definition_name };
	List definition_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &definition_cell };
	CreatePLangStmt language = { .type = T_CreatePLangStmt,
								 .plname = "target_language",
								 .plhandler = &definition_names };
	CreateConversionStmt conversion = { .type = T_CreateConversionStmt,
										.conversion_name = &definition_names,
										.for_encoding_name = "UTF8",
										.to_encoding_name = "LATIN1",
										.func_name = &definition_names };
	TypeName source_type = { .type = T_TypeName, .typeOid = INT4OID, .typemod = -1 };
	TypeName target_type = { .type = T_TypeName, .typeOid = TEXTOID, .typemod = -1 };
	CreateCastStmt cast = { .type = T_CreateCastStmt,
							.sourcetype = &source_type,
							.targettype = &target_type,
							.context = COERCION_EXPLICIT,
							.inout = true };
	ObjectWithArgs transform_function = { .type = T_ObjectWithArgs, .objname = &definition_names };
	CreateTransformStmt transform = { .type = T_CreateTransformStmt,
									  .type_name = &source_type,
									  .lang = "target_language",
									  .fromsql = &transform_function };
	CreateAmStmt access_method = { .type = T_CreateAmStmt,
								   .amname = "target_am",
								   .handler_name = &definition_names,
								   .amtype = AMTYPE_INDEX };
	Node *statements[] = { (Node *)&schema,
						   (Node *)&sequence,
						   (Node *)&index,
						   (Node *)&view,
						   (Node *)&rename,
						   (Node *)&move,
						   (Node *)&grant,
						   (Node *)&role,
						   (Node *)&defaults,
						   (Node *)&create_function,
						   (Node *)&create_procedure,
						   (Node *)&alter_function,
						   (Node *)&alter_procedure,
						   (Node *)&alter_routine,
						   (Node *)&trigger,
						   (Node *)&rule,
						   (Node *)&policy,
						   (Node *)&alter_policy,
						   (Node *)&comment,
						   (Node *)&statistics,
						   (Node *)&create_event,
						   (Node *)&alter_event,
						   (Node *)&alter_collation,
						   (Node *)&aggregate,
						   (Node *)&collation,
						   (Node *)&parser,
						   (Node *)&dictionary,
						   (Node *)&template,
						   (Node *)&configuration,
						   (Node *)&create_stats,
						   (Node *)&alter_sequence,
						   (Node *)&alter_dictionary,
						   (Node *)&mapping_add,
						   (Node *)&mapping_alter,
						   (Node *)&mapping_replace,
						   (Node *)&mapping_replace_token,
						   (Node *)&mapping_drop,
						   (Node *)&create_role,
						   (Node *)&create_user,
						   (Node *)&create_group,
						   (Node *)&alter_role,
						   (Node *)&alter_settings,
						   (Node *)&all_settings,
						   (Node *)&drop_role,
						   (Node *)&database_settings,
						   (Node *)&database_collation,
						   (Node *)&language,
						   (Node *)&conversion,
						   (Node *)&cast,
						   (Node *)&transform,
						   (Node *)&access_method };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		int caught;

		reset_create_preflight();
		utility_bind_ok = false;
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			create_sql_entry(statements[i]);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(error_code, ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_prepares, 1);
		UT_ASSERT_EQ(utility_binds, 0);
		UT_ASSERT_EQ(utility_after_preflight, 0);
	}
}

UT_TEST(test_catalog_writes_child_or_wrong_top_refuses_before_allocation)
{
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt };
	CreateSeqStmt sequence = { .type = T_CreateSeqStmt };
	IndexStmt index = { .type = T_IndexStmt };
	ViewStmt view = { .type = T_ViewStmt };
	RenameStmt rename
		= { .type = T_RenameStmt, .renameType = OBJECT_TABLE, .relationType = OBJECT_TABLE };
	AlterObjectSchemaStmt move = { .type = T_AlterObjectSchemaStmt, .objectType = OBJECT_TABLE };
	GrantStmt grant = { .type = T_GrantStmt,
						.is_grant = true,
						.targtype = ACL_TARGET_OBJECT,
						.objtype = OBJECT_TABLE };
	GrantRoleStmt role = { .type = T_GrantRoleStmt, .is_grant = true };
	GrantStmt default_action = { .type = T_GrantStmt,
								 .is_grant = true,
								 .targtype = ACL_TARGET_DEFAULTS,
								 .objtype = OBJECT_TABLE };
	AlterDefaultPrivilegesStmt defaults
		= { .type = T_AlterDefaultPrivilegesStmt, .action = &default_action };
	CreateFunctionStmt create_function = { .type = T_CreateFunctionStmt };
	CreateFunctionStmt create_procedure = { .type = T_CreateFunctionStmt, .is_procedure = true };
	AlterFunctionStmt alter_function = { .type = T_AlterFunctionStmt, .objtype = OBJECT_FUNCTION };
	AlterFunctionStmt alter_procedure
		= { .type = T_AlterFunctionStmt, .objtype = OBJECT_PROCEDURE };
	AlterFunctionStmt alter_routine = { .type = T_AlterFunctionStmt, .objtype = OBJECT_ROUTINE };
	CreateTrigStmt trigger = { .type = T_CreateTrigStmt };
	RuleStmt rule = { .type = T_RuleStmt, .event = CMD_UPDATE };
	CreatePolicyStmt policy = { .type = T_CreatePolicyStmt, .permissive = true };
	AlterPolicyStmt alter_policy = { .type = T_AlterPolicyStmt };
	CommentStmt comment = { .type = T_CommentStmt, .objtype = OBJECT_TABLE };
	AlterStatsStmt statistics = { .type = T_AlterStatsStmt, .stxstattarget = 100 };
	CreateEventTrigStmt create_event
		= { .type = T_CreateEventTrigStmt, .eventname = "ddl_command_start" };
	AlterEventTrigStmt alter_event = { .type = T_AlterEventTrigStmt, .tgenabled = 'O' };
	AlterCollationStmt alter_collation = { .type = T_AlterCollationStmt };
	DefineStmt aggregate = { .type = T_DefineStmt, .kind = OBJECT_AGGREGATE };
	DefineStmt collation = { .type = T_DefineStmt, .kind = OBJECT_COLLATION };
	DefineStmt parser = { .type = T_DefineStmt, .kind = OBJECT_TSPARSER };
	DefineStmt dictionary = { .type = T_DefineStmt, .kind = OBJECT_TSDICTIONARY };
	DefineStmt template = { .type = T_DefineStmt, .kind = OBJECT_TSTEMPLATE };
	DefineStmt configuration = { .type = T_DefineStmt, .kind = OBJECT_TSCONFIGURATION };
	RangeVar stats_relation = { .type = T_RangeVar, .relname = "target" };
	ListCell stats_cell = { .ptr_value = &stats_relation };
	List stats_relations
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &stats_cell };
	CreateStatsStmt create_stats = { .type = T_CreateStatsStmt, .relations = &stats_relations };
	AlterSeqStmt alter_sequence = { .type = T_AlterSeqStmt };
	AlterTSDictionaryStmt alter_dictionary = { .type = T_AlterTSDictionaryStmt };
	AlterTSConfigurationStmt mapping_add
		= { .type = T_AlterTSConfigurationStmt, .kind = ALTER_TSCONFIG_ADD_MAPPING };
	AlterTSConfigurationStmt mapping_alter = { .type = T_AlterTSConfigurationStmt,
											   .kind = ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN,
											   .override = true };
	AlterTSConfigurationStmt mapping_replace = { .type = T_AlterTSConfigurationStmt,
												 .kind = ALTER_TSCONFIG_REPLACE_DICT,
												 .replace = true };
	AlterTSConfigurationStmt mapping_replace_token
		= { .type = T_AlterTSConfigurationStmt,
			.kind = ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN,
			.replace = true };
	AlterTSConfigurationStmt mapping_drop
		= { .type = T_AlterTSConfigurationStmt, .kind = ALTER_TSCONFIG_DROP_MAPPING };
	RoleSpec role_target
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "target" };
	CreateRoleStmt create_role
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_ROLE, .role = "target" };
	CreateRoleStmt create_user
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_USER, .role = "target_user" };
	CreateRoleStmt create_group
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_GROUP, .role = "target_group" };
	AlterRoleStmt alter_role = { .type = T_AlterRoleStmt, .role = &role_target, .action = 1 };
	VariableSetStmt role_setting
		= { .type = T_VariableSetStmt, .kind = VAR_RESET, .name = "application_name" };
	AlterRoleSetStmt alter_settings
		= { .type = T_AlterRoleSetStmt, .role = &role_target, .setstmt = &role_setting };
	AlterRoleSetStmt all_settings
		= { .type = T_AlterRoleSetStmt, .database = "target_database", .setstmt = &role_setting };
	ListCell role_cell = { .ptr_value = &role_target };
	List roles = { .type = T_List, .length = 1, .max_length = 1, .elements = &role_cell };
	DropRoleStmt drop_role = { .type = T_DropRoleStmt, .roles = &roles };
	VariableSetStmt database_setting
		= { .type = T_VariableSetStmt, .kind = VAR_RESET, .name = "application_name" };
	AlterDatabaseSetStmt database_settings = { .type = T_AlterDatabaseSetStmt,
											   .dbname = "target_database",
											   .setstmt = &database_setting };
	AlterDatabaseRefreshCollStmt database_collation
		= { .type = T_AlterDatabaseRefreshCollStmt, .dbname = "target_database" };
	String definition_name = { .type = T_String, .sval = "target_handler" };
	ListCell definition_cell = { .ptr_value = &definition_name };
	List definition_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &definition_cell };
	CreatePLangStmt language = { .type = T_CreatePLangStmt,
								 .plname = "target_language",
								 .plhandler = &definition_names };
	CreateConversionStmt conversion = { .type = T_CreateConversionStmt,
										.conversion_name = &definition_names,
										.for_encoding_name = "UTF8",
										.to_encoding_name = "LATIN1",
										.func_name = &definition_names };
	TypeName source_type = { .type = T_TypeName, .typeOid = INT4OID, .typemod = -1 };
	TypeName target_type = { .type = T_TypeName, .typeOid = TEXTOID, .typemod = -1 };
	CreateCastStmt cast = { .type = T_CreateCastStmt,
							.sourcetype = &source_type,
							.targettype = &target_type,
							.context = COERCION_EXPLICIT,
							.inout = true };
	ObjectWithArgs transform_function = { .type = T_ObjectWithArgs, .objname = &definition_names };
	CreateTransformStmt transform = { .type = T_CreateTransformStmt,
									  .type_name = &source_type,
									  .lang = "target_language",
									  .fromsql = &transform_function };
	CreateAmStmt access_method = { .type = T_CreateAmStmt,
								   .amname = "target_am",
								   .handler_name = &definition_names,
								   .amtype = AMTYPE_INDEX };
	Node *statements[] = { (Node *)&schema,
						   (Node *)&sequence,
						   (Node *)&index,
						   (Node *)&view,
						   (Node *)&rename,
						   (Node *)&move,
						   (Node *)&grant,
						   (Node *)&role,
						   (Node *)&defaults,
						   (Node *)&create_function,
						   (Node *)&create_procedure,
						   (Node *)&alter_function,
						   (Node *)&alter_procedure,
						   (Node *)&alter_routine,
						   (Node *)&trigger,
						   (Node *)&rule,
						   (Node *)&policy,
						   (Node *)&alter_policy,
						   (Node *)&comment,
						   (Node *)&statistics,
						   (Node *)&create_event,
						   (Node *)&alter_event,
						   (Node *)&alter_collation,
						   (Node *)&aggregate,
						   (Node *)&collation,
						   (Node *)&parser,
						   (Node *)&dictionary,
						   (Node *)&template,
						   (Node *)&configuration,
						   (Node *)&create_stats,
						   (Node *)&alter_sequence,
						   (Node *)&alter_dictionary,
						   (Node *)&mapping_add,
						   (Node *)&mapping_alter,
						   (Node *)&mapping_replace,
						   (Node *)&mapping_replace_token,
						   (Node *)&mapping_drop,
						   (Node *)&create_role,
						   (Node *)&create_user,
						   (Node *)&create_group,
						   (Node *)&alter_role,
						   (Node *)&alter_settings,
						   (Node *)&all_settings,
						   (Node *)&drop_role,
						   (Node *)&database_settings,
						   (Node *)&database_collation,
						   (Node *)&language,
						   (Node *)&conversion,
						   (Node *)&cast,
						   (Node *)&transform,
						   (Node *)&access_method };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		for (unsigned child = 0; child < 2; child++) {
			int caught;

			reset_create_preflight();
			utility_in_child = child != 0;
			if (!child) {
				utility_top_xid = 400;
				utility_current_xid = 401;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				create_sql_entry(statements[i]);
			UT_ASSERT(caught);
			UT_ASSERT_EQ(error_code,
						 child ? ERRCODE_FEATURE_NOT_SUPPORTED : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_after_preflight, 0);
		}
	}
}

UT_TEST(test_catalog_writes_preflight_requires_shared_catalog_storage)
{
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt };
	CreateSeqStmt sequence = { .type = T_CreateSeqStmt };
	IndexStmt index = { .type = T_IndexStmt };
	ViewStmt view = { .type = T_ViewStmt };
	RenameStmt rename
		= { .type = T_RenameStmt, .renameType = OBJECT_TABLE, .relationType = OBJECT_TABLE };
	AlterObjectSchemaStmt move = { .type = T_AlterObjectSchemaStmt, .objectType = OBJECT_TABLE };
	GrantStmt grant = { .type = T_GrantStmt,
						.is_grant = true,
						.targtype = ACL_TARGET_OBJECT,
						.objtype = OBJECT_TABLE };
	GrantRoleStmt role = { .type = T_GrantRoleStmt, .is_grant = true };
	GrantStmt default_action = { .type = T_GrantStmt,
								 .is_grant = true,
								 .targtype = ACL_TARGET_DEFAULTS,
								 .objtype = OBJECT_TABLE };
	AlterDefaultPrivilegesStmt defaults
		= { .type = T_AlterDefaultPrivilegesStmt, .action = &default_action };
	CreateFunctionStmt create_function = { .type = T_CreateFunctionStmt };
	CreateFunctionStmt create_procedure = { .type = T_CreateFunctionStmt, .is_procedure = true };
	AlterFunctionStmt alter_function = { .type = T_AlterFunctionStmt, .objtype = OBJECT_FUNCTION };
	AlterFunctionStmt alter_procedure
		= { .type = T_AlterFunctionStmt, .objtype = OBJECT_PROCEDURE };
	AlterFunctionStmt alter_routine = { .type = T_AlterFunctionStmt, .objtype = OBJECT_ROUTINE };
	CreateTrigStmt trigger = { .type = T_CreateTrigStmt };
	RuleStmt rule = { .type = T_RuleStmt, .event = CMD_UPDATE };
	CreatePolicyStmt policy = { .type = T_CreatePolicyStmt, .permissive = true };
	AlterPolicyStmt alter_policy = { .type = T_AlterPolicyStmt };
	CommentStmt comment = { .type = T_CommentStmt, .objtype = OBJECT_TABLE };
	AlterStatsStmt statistics = { .type = T_AlterStatsStmt, .stxstattarget = 100 };
	CreateEventTrigStmt create_event
		= { .type = T_CreateEventTrigStmt, .eventname = "ddl_command_start" };
	AlterEventTrigStmt alter_event = { .type = T_AlterEventTrigStmt, .tgenabled = 'O' };
	AlterCollationStmt alter_collation = { .type = T_AlterCollationStmt };
	DefineStmt aggregate = { .type = T_DefineStmt, .kind = OBJECT_AGGREGATE };
	DefineStmt collation = { .type = T_DefineStmt, .kind = OBJECT_COLLATION };
	DefineStmt parser = { .type = T_DefineStmt, .kind = OBJECT_TSPARSER };
	DefineStmt dictionary = { .type = T_DefineStmt, .kind = OBJECT_TSDICTIONARY };
	DefineStmt template = { .type = T_DefineStmt, .kind = OBJECT_TSTEMPLATE };
	DefineStmt configuration = { .type = T_DefineStmt, .kind = OBJECT_TSCONFIGURATION };
	RangeVar stats_relation = { .type = T_RangeVar, .relname = "target" };
	ListCell stats_cell = { .ptr_value = &stats_relation };
	List stats_relations
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &stats_cell };
	CreateStatsStmt create_stats = { .type = T_CreateStatsStmt, .relations = &stats_relations };
	AlterSeqStmt alter_sequence = { .type = T_AlterSeqStmt };
	AlterTSDictionaryStmt alter_dictionary = { .type = T_AlterTSDictionaryStmt };
	AlterTSConfigurationStmt mapping_add
		= { .type = T_AlterTSConfigurationStmt, .kind = ALTER_TSCONFIG_ADD_MAPPING };
	AlterTSConfigurationStmt mapping_alter = { .type = T_AlterTSConfigurationStmt,
											   .kind = ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN,
											   .override = true };
	AlterTSConfigurationStmt mapping_replace = { .type = T_AlterTSConfigurationStmt,
												 .kind = ALTER_TSCONFIG_REPLACE_DICT,
												 .replace = true };
	AlterTSConfigurationStmt mapping_replace_token
		= { .type = T_AlterTSConfigurationStmt,
			.kind = ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN,
			.replace = true };
	AlterTSConfigurationStmt mapping_drop
		= { .type = T_AlterTSConfigurationStmt, .kind = ALTER_TSCONFIG_DROP_MAPPING };
	RoleSpec role_target
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "target" };
	CreateRoleStmt create_role
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_ROLE, .role = "target" };
	CreateRoleStmt create_user
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_USER, .role = "target_user" };
	CreateRoleStmt create_group
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_GROUP, .role = "target_group" };
	AlterRoleStmt alter_role = { .type = T_AlterRoleStmt, .role = &role_target, .action = 1 };
	VariableSetStmt role_setting
		= { .type = T_VariableSetStmt, .kind = VAR_RESET, .name = "application_name" };
	AlterRoleSetStmt alter_settings
		= { .type = T_AlterRoleSetStmt, .role = &role_target, .setstmt = &role_setting };
	AlterRoleSetStmt all_settings
		= { .type = T_AlterRoleSetStmt, .database = "target_database", .setstmt = &role_setting };
	ListCell role_cell = { .ptr_value = &role_target };
	List roles = { .type = T_List, .length = 1, .max_length = 1, .elements = &role_cell };
	DropRoleStmt drop_role = { .type = T_DropRoleStmt, .roles = &roles };
	VariableSetStmt database_setting
		= { .type = T_VariableSetStmt, .kind = VAR_RESET, .name = "application_name" };
	AlterDatabaseSetStmt database_settings = { .type = T_AlterDatabaseSetStmt,
											   .dbname = "target_database",
											   .setstmt = &database_setting };
	AlterDatabaseRefreshCollStmt database_collation
		= { .type = T_AlterDatabaseRefreshCollStmt, .dbname = "target_database" };
	String definition_name = { .type = T_String, .sval = "target_handler" };
	ListCell definition_cell = { .ptr_value = &definition_name };
	List definition_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &definition_cell };
	CreatePLangStmt language = { .type = T_CreatePLangStmt,
								 .plname = "target_language",
								 .plhandler = &definition_names };
	CreateConversionStmt conversion = { .type = T_CreateConversionStmt,
										.conversion_name = &definition_names,
										.for_encoding_name = "UTF8",
										.to_encoding_name = "LATIN1",
										.func_name = &definition_names };
	TypeName source_type = { .type = T_TypeName, .typeOid = INT4OID, .typemod = -1 };
	TypeName target_type = { .type = T_TypeName, .typeOid = TEXTOID, .typemod = -1 };
	CreateCastStmt cast = { .type = T_CreateCastStmt,
							.sourcetype = &source_type,
							.targettype = &target_type,
							.context = COERCION_EXPLICIT,
							.inout = true };
	ObjectWithArgs transform_function = { .type = T_ObjectWithArgs, .objname = &definition_names };
	CreateTransformStmt transform = { .type = T_CreateTransformStmt,
									  .type_name = &source_type,
									  .lang = "target_language",
									  .fromsql = &transform_function };
	CreateAmStmt access_method = { .type = T_CreateAmStmt,
								   .amname = "target_am",
								   .handler_name = &definition_names,
								   .amtype = AMTYPE_INDEX };
	Node *statements[] = { (Node *)&schema,
						   (Node *)&sequence,
						   (Node *)&index,
						   (Node *)&view,
						   (Node *)&rename,
						   (Node *)&move,
						   (Node *)&grant,
						   (Node *)&role,
						   (Node *)&defaults,
						   (Node *)&create_function,
						   (Node *)&create_procedure,
						   (Node *)&alter_function,
						   (Node *)&alter_procedure,
						   (Node *)&alter_routine,
						   (Node *)&trigger,
						   (Node *)&rule,
						   (Node *)&policy,
						   (Node *)&alter_policy,
						   (Node *)&comment,
						   (Node *)&statistics,
						   (Node *)&create_event,
						   (Node *)&alter_event,
						   (Node *)&alter_collation,
						   (Node *)&aggregate,
						   (Node *)&collation,
						   (Node *)&parser,
						   (Node *)&dictionary,
						   (Node *)&template,
						   (Node *)&configuration,
						   (Node *)&create_stats,
						   (Node *)&alter_sequence,
						   (Node *)&alter_dictionary,
						   (Node *)&mapping_add,
						   (Node *)&mapping_alter,
						   (Node *)&mapping_replace,
						   (Node *)&mapping_replace_token,
						   (Node *)&mapping_drop,
						   (Node *)&create_role,
						   (Node *)&create_user,
						   (Node *)&create_group,
						   (Node *)&alter_role,
						   (Node *)&alter_settings,
						   (Node *)&all_settings,
						   (Node *)&drop_role,
						   (Node *)&database_settings,
						   (Node *)&database_collation,
						   (Node *)&language,
						   (Node *)&conversion,
						   (Node *)&cast,
						   (Node *)&transform,
						   (Node *)&access_method };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			reset_create_preflight();
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			create_sql_entry(statements[i]);
			UT_ASSERT_EQ(utility_after_preflight, 1);
			UT_ASSERT_EQ(utility_xids, (mode & 3) == 3);
			UT_ASSERT_EQ(utility_prepares, (mode & 3) == 3);
			UT_ASSERT_EQ(utility_binds, (mode & 3) == 3);
		}
	}
}

UT_TEST(test_catalog_writes_readonly_parallel_guards_precede_binding)
{
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt };
	CreateSeqStmt sequence = { .type = T_CreateSeqStmt };
	IndexStmt index = { .type = T_IndexStmt };
	ViewStmt view = { .type = T_ViewStmt };
	RenameStmt rename
		= { .type = T_RenameStmt, .renameType = OBJECT_TABLE, .relationType = OBJECT_TABLE };
	AlterObjectSchemaStmt move = { .type = T_AlterObjectSchemaStmt, .objectType = OBJECT_TABLE };
	GrantStmt grant = { .type = T_GrantStmt,
						.is_grant = true,
						.targtype = ACL_TARGET_OBJECT,
						.objtype = OBJECT_TABLE };
	GrantRoleStmt role = { .type = T_GrantRoleStmt, .is_grant = true };
	GrantStmt default_action = { .type = T_GrantStmt,
								 .is_grant = true,
								 .targtype = ACL_TARGET_DEFAULTS,
								 .objtype = OBJECT_TABLE };
	AlterDefaultPrivilegesStmt defaults
		= { .type = T_AlterDefaultPrivilegesStmt, .action = &default_action };
	CreateFunctionStmt create_function = { .type = T_CreateFunctionStmt };
	CreateFunctionStmt create_procedure = { .type = T_CreateFunctionStmt, .is_procedure = true };
	AlterFunctionStmt alter_function = { .type = T_AlterFunctionStmt, .objtype = OBJECT_FUNCTION };
	AlterFunctionStmt alter_procedure
		= { .type = T_AlterFunctionStmt, .objtype = OBJECT_PROCEDURE };
	AlterFunctionStmt alter_routine = { .type = T_AlterFunctionStmt, .objtype = OBJECT_ROUTINE };
	CreateTrigStmt trigger = { .type = T_CreateTrigStmt };
	RuleStmt rule = { .type = T_RuleStmt, .event = CMD_UPDATE };
	CreatePolicyStmt policy = { .type = T_CreatePolicyStmt, .permissive = true };
	AlterPolicyStmt alter_policy = { .type = T_AlterPolicyStmt };
	CommentStmt comment = { .type = T_CommentStmt, .objtype = OBJECT_TABLE };
	AlterStatsStmt statistics = { .type = T_AlterStatsStmt, .stxstattarget = 100 };
	CreateEventTrigStmt create_event
		= { .type = T_CreateEventTrigStmt, .eventname = "ddl_command_start" };
	AlterEventTrigStmt alter_event = { .type = T_AlterEventTrigStmt, .tgenabled = 'O' };
	AlterCollationStmt alter_collation = { .type = T_AlterCollationStmt };
	DefineStmt aggregate = { .type = T_DefineStmt, .kind = OBJECT_AGGREGATE };
	DefineStmt collation = { .type = T_DefineStmt, .kind = OBJECT_COLLATION };
	DefineStmt parser = { .type = T_DefineStmt, .kind = OBJECT_TSPARSER };
	DefineStmt dictionary = { .type = T_DefineStmt, .kind = OBJECT_TSDICTIONARY };
	DefineStmt template = { .type = T_DefineStmt, .kind = OBJECT_TSTEMPLATE };
	DefineStmt configuration = { .type = T_DefineStmt, .kind = OBJECT_TSCONFIGURATION };
	RangeVar stats_relation = { .type = T_RangeVar, .relname = "target" };
	ListCell stats_cell = { .ptr_value = &stats_relation };
	List stats_relations
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &stats_cell };
	CreateStatsStmt create_stats = { .type = T_CreateStatsStmt, .relations = &stats_relations };
	AlterSeqStmt alter_sequence = { .type = T_AlterSeqStmt };
	AlterTSDictionaryStmt alter_dictionary = { .type = T_AlterTSDictionaryStmt };
	AlterTSConfigurationStmt mapping_add
		= { .type = T_AlterTSConfigurationStmt, .kind = ALTER_TSCONFIG_ADD_MAPPING };
	AlterTSConfigurationStmt mapping_alter = { .type = T_AlterTSConfigurationStmt,
											   .kind = ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN,
											   .override = true };
	AlterTSConfigurationStmt mapping_replace = { .type = T_AlterTSConfigurationStmt,
												 .kind = ALTER_TSCONFIG_REPLACE_DICT,
												 .replace = true };
	AlterTSConfigurationStmt mapping_replace_token
		= { .type = T_AlterTSConfigurationStmt,
			.kind = ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN,
			.replace = true };
	AlterTSConfigurationStmt mapping_drop
		= { .type = T_AlterTSConfigurationStmt, .kind = ALTER_TSCONFIG_DROP_MAPPING };
	RoleSpec role_target
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "target" };
	CreateRoleStmt create_role
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_ROLE, .role = "target" };
	CreateRoleStmt create_user
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_USER, .role = "target_user" };
	CreateRoleStmt create_group
		= { .type = T_CreateRoleStmt, .stmt_type = ROLESTMT_GROUP, .role = "target_group" };
	AlterRoleStmt alter_role = { .type = T_AlterRoleStmt, .role = &role_target, .action = 1 };
	VariableSetStmt role_setting
		= { .type = T_VariableSetStmt, .kind = VAR_RESET, .name = "application_name" };
	AlterRoleSetStmt alter_settings
		= { .type = T_AlterRoleSetStmt, .role = &role_target, .setstmt = &role_setting };
	AlterRoleSetStmt all_settings
		= { .type = T_AlterRoleSetStmt, .database = "target_database", .setstmt = &role_setting };
	ListCell role_cell = { .ptr_value = &role_target };
	List roles = { .type = T_List, .length = 1, .max_length = 1, .elements = &role_cell };
	DropRoleStmt drop_role = { .type = T_DropRoleStmt, .roles = &roles };
	VariableSetStmt database_setting
		= { .type = T_VariableSetStmt, .kind = VAR_RESET, .name = "application_name" };
	AlterDatabaseSetStmt database_settings = { .type = T_AlterDatabaseSetStmt,
											   .dbname = "target_database",
											   .setstmt = &database_setting };
	AlterDatabaseRefreshCollStmt database_collation
		= { .type = T_AlterDatabaseRefreshCollStmt, .dbname = "target_database" };
	String definition_name = { .type = T_String, .sval = "target_handler" };
	ListCell definition_cell = { .ptr_value = &definition_name };
	List definition_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &definition_cell };
	CreatePLangStmt language = { .type = T_CreatePLangStmt,
								 .plname = "target_language",
								 .plhandler = &definition_names };
	CreateConversionStmt conversion = { .type = T_CreateConversionStmt,
										.conversion_name = &definition_names,
										.for_encoding_name = "UTF8",
										.to_encoding_name = "LATIN1",
										.func_name = &definition_names };
	TypeName source_type = { .type = T_TypeName, .typeOid = INT4OID, .typemod = -1 };
	TypeName target_type = { .type = T_TypeName, .typeOid = TEXTOID, .typemod = -1 };
	CreateCastStmt cast = { .type = T_CreateCastStmt,
							.sourcetype = &source_type,
							.targettype = &target_type,
							.context = COERCION_EXPLICIT,
							.inout = true };
	ObjectWithArgs transform_function = { .type = T_ObjectWithArgs, .objname = &definition_names };
	CreateTransformStmt transform = { .type = T_CreateTransformStmt,
									  .type_name = &source_type,
									  .lang = "target_language",
									  .fromsql = &transform_function };
	CreateAmStmt access_method = { .type = T_CreateAmStmt,
								   .amname = "target_am",
								   .handler_name = &definition_names,
								   .amtype = AMTYPE_INDEX };
	Node *statements[] = { (Node *)&schema,
						   (Node *)&sequence,
						   (Node *)&index,
						   (Node *)&view,
						   (Node *)&rename,
						   (Node *)&move,
						   (Node *)&grant,
						   (Node *)&role,
						   (Node *)&defaults,
						   (Node *)&create_function,
						   (Node *)&create_procedure,
						   (Node *)&alter_function,
						   (Node *)&alter_procedure,
						   (Node *)&alter_routine,
						   (Node *)&trigger,
						   (Node *)&rule,
						   (Node *)&policy,
						   (Node *)&alter_policy,
						   (Node *)&comment,
						   (Node *)&statistics,
						   (Node *)&create_event,
						   (Node *)&alter_event,
						   (Node *)&alter_collation,
						   (Node *)&aggregate,
						   (Node *)&collation,
						   (Node *)&parser,
						   (Node *)&dictionary,
						   (Node *)&template,
						   (Node *)&configuration,
						   (Node *)&create_stats,
						   (Node *)&alter_sequence,
						   (Node *)&alter_dictionary,
						   (Node *)&mapping_add,
						   (Node *)&mapping_alter,
						   (Node *)&mapping_replace,
						   (Node *)&mapping_replace_token,
						   (Node *)&mapping_drop,
						   (Node *)&create_role,
						   (Node *)&create_user,
						   (Node *)&create_group,
						   (Node *)&alter_role,
						   (Node *)&alter_settings,
						   (Node *)&all_settings,
						   (Node *)&drop_role,
						   (Node *)&database_settings,
						   (Node *)&database_collation,
						   (Node *)&language,
						   (Node *)&conversion,
						   (Node *)&cast,
						   (Node *)&transform,
						   (Node *)&access_method };
	const char *tags[] = { "CREATE SCHEMA",
						   "CREATE SEQUENCE",
						   "CREATE INDEX",
						   "CREATE VIEW",
						   "ALTER TABLE",
						   "ALTER TABLE",
						   "GRANT",
						   "GRANT ROLE",
						   "ALTER DEFAULT PRIVILEGES",
						   "CREATE FUNCTION",
						   "CREATE PROCEDURE",
						   "ALTER FUNCTION",
						   "ALTER PROCEDURE",
						   "ALTER ROUTINE",
						   "CREATE TRIGGER",
						   "CREATE RULE",
						   "CREATE POLICY",
						   "ALTER POLICY",
						   "COMMENT",
						   "ALTER STATISTICS",
						   "CREATE EVENT TRIGGER",
						   "ALTER EVENT TRIGGER",
						   "ALTER COLLATION",
						   "CREATE AGGREGATE",
						   "CREATE COLLATION",
						   "CREATE TEXT SEARCH PARSER",
						   "CREATE TEXT SEARCH DICTIONARY",
						   "CREATE TEXT SEARCH TEMPLATE",
						   "CREATE TEXT SEARCH CONFIGURATION",
						   "CREATE STATISTICS",
						   "ALTER SEQUENCE",
						   "ALTER TEXT SEARCH DICTIONARY",
						   "ALTER TEXT SEARCH CONFIGURATION",
						   "ALTER TEXT SEARCH CONFIGURATION",
						   "ALTER TEXT SEARCH CONFIGURATION",
						   "ALTER TEXT SEARCH CONFIGURATION",
						   "ALTER TEXT SEARCH CONFIGURATION",
						   "CREATE ROLE",
						   "CREATE ROLE",
						   "CREATE ROLE",
						   "ALTER ROLE",
						   "ALTER ROLE",
						   "ALTER ROLE",
						   "DROP ROLE",
						   "ALTER DATABASE",
						   "ALTER DATABASE",
						   "CREATE LANGUAGE",
						   "CREATE CONVERSION",
						   "CREATE CAST",
						   "CREATE TRANSFORM",
						   "CREATE ACCESS METHOD" };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		for (unsigned parallel = 0; parallel < 2; parallel++) {
			int caught;

			reset_create_preflight();
			XactReadOnly = parallel == 0;
			utility_parallel = parallel != 0;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				create_sql_entry(statements[i]);
			UT_ASSERT(caught);
			UT_ASSERT_EQ(error_code, parallel ? ERRCODE_INVALID_TRANSACTION_STATE
											  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT(strstr(error_message, tags[i]) != NULL);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_after_preflight, 0);
		}
	}
}

UT_TEST(test_schema_unsupported_children_refuse_before_binding)
{
	RangeVar target = { .type = T_RangeVar, .relpersistence = RELPERSISTENCE_UNLOGGED };
	CreateStmt table = { .type = T_CreateStmt, .relation = &target };
	IndexStmt index = { .type = T_IndexStmt, .accessMethod = "hash" };
	Node *statements[] = { (Node *)&table, (Node *)&index };
	ListCell cell = { .ptr_value = NULL };
	List children = { .type = T_List, .length = 1, .max_length = 1, .elements = &cell };
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt, .schemaElts = &children };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		int caught;

		reset_create_preflight();
		cell.ptr_value = statements[i];
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			create_sql_entry((Node *)&schema);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(utility_xids, 0);
		UT_ASSERT_EQ(utility_prepares, 0);
		UT_ASSERT_EQ(utility_after_preflight, 0);
	}
}

UT_TEST(test_unlogged_sequence_refuses_before_binding_only_in_shared_catalog)
{
	RangeVar target = { .type = T_RangeVar, .relpersistence = RELPERSISTENCE_UNLOGGED };
	CreateSeqStmt sequence = { .type = T_CreateSeqStmt, .sequence = &target };
	ListCell cell = { .ptr_value = &sequence };
	List children = { .type = T_List, .length = 1, .max_length = 1, .elements = &cell };
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt, .schemaElts = &children };
	Node *statements[] = { (Node *)&sequence, (Node *)&schema };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		for (unsigned mode = 0; mode < 4; mode++) {
			int caught;

			reset_create_preflight();
			cluster_shared_catalog = (mode & 1) != 0;
			cluster_shared_config = (mode & 2) != 0;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				create_sql_entry(statements[i]);
			UT_ASSERT_EQ(caught != 0, cluster_shared_catalog);
			UT_ASSERT_EQ(error_code, cluster_shared_catalog ? ERRCODE_FEATURE_NOT_SUPPORTED : 0);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_after_preflight, cluster_shared_catalog ? 0 : 1);
		}
	}
}

UT_TEST(test_unsupported_index_refuses_before_binding_and_schema_work)
{
	IndexStmt index = { .type = T_IndexStmt };
	ListCell cell = { .ptr_value = &index };
	List children = { .type = T_List, .length = 1, .max_length = 1, .elements = &cell };
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt, .schemaElts = &children };
	Node *statements[] = { (Node *)&index, (Node *)&schema };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		for (unsigned concurrent = 0; concurrent < 2; concurrent++) {
			int caught;

			reset_create_preflight();
			index.accessMethod = concurrent ? "btree" : "hash";
			index.concurrent = concurrent != 0;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				create_sql_entry(statements[i]);
			UT_ASSERT(caught);
			UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_after_preflight, 0);
		}
	}
}

/* Unsupported FROM forms keep their original native validation path.
 * This prologue boundary proves no early allocation, not the later error. */
UT_TEST(test_unsupported_statistics_from_keeps_native_path_without_binding)
{
	RangeVar targets[2] = { { .type = T_RangeVar }, { .type = T_RangeVar } };
	JoinExpr join = { .type = T_JoinExpr,
					  .jointype = JOIN_INNER,
					  .larg = (Node *)&targets[0],
					  .rarg = (Node *)&targets[1] };
	RangeSubselect subquery = { .type = T_RangeSubselect };
	Node *froms[] = { (Node *)&targets[0], (Node *)&join, (Node *)&subquery };
	ListCell cells[2] = { { .ptr_value = &targets[0] }, { .ptr_value = &targets[1] } };
	List relations = { .type = T_List, .length = 2, .max_length = 2, .elements = cells };
	CreateStatsStmt statistics = { .type = T_CreateStatsStmt, .relations = &relations };

	for (unsigned variant = 0; variant < lengthof(froms); variant++) {
		cells[0].ptr_value = froms[variant];
		relations.length = relations.max_length = variant == 0 ? 2 : 1;
		for (unsigned skip = 0; skip < 2; skip++) {
			reset_create_preflight();
			statistics.if_not_exists = skip != 0;
			create_sql_entry((Node *)&statistics);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_binds, 0);
			UT_ASSERT_EQ(utility_after_preflight, 1);
		}
	}
}

int
main(void)
{
	UT_PLAN(45);
	UT_RUN(test_unsupported_rename_or_schema_move_refuses_before_binding);
	UT_RUN(test_unsupported_index_refuses_before_binding_and_schema_work);
	UT_RUN(test_catalog_writes_bind_before_later_work_and_reuse_top);
	UT_RUN(test_schema_binds_before_catalog_children_and_children_reuse_top);
	UT_RUN(test_catalog_writes_publication_refusal_precedes_later_work);
	UT_RUN(test_catalog_writes_child_or_wrong_top_refuses_before_allocation);
	UT_RUN(test_catalog_writes_preflight_requires_shared_catalog_storage);
	UT_RUN(test_catalog_writes_readonly_parallel_guards_precede_binding);
	UT_RUN(test_schema_unsupported_children_refuse_before_binding);
	UT_RUN(test_unlogged_sequence_refuses_before_binding_only_in_shared_catalog);
	UT_RUN(test_shared_truncate_variants_and_targets_bind_before_later_work);
	UT_RUN(test_truncate_reuses_existing_top_binding_across_utility_calls);
	UT_RUN(test_truncate_publication_refusal_precedes_later_utility_work);
	UT_RUN(test_truncate_child_or_wrong_top_refuses_before_allocation);
	UT_RUN(test_truncate_preflight_requires_shared_catalog_storage);
	UT_RUN(test_truncate_original_readonly_parallel_guards_precede_binding);
	UT_RUN(test_pre1_startup_refuses_before_root_and_without_writes);
	UT_RUN(test_selected_pre1_image_and_corruption_are_distinct);
	UT_RUN(test_nonshared_startup_remains_native);
	UT_RUN(test_catalog_only_profile_refuses_pre1);
	UT_RUN(test_bound_startup_does_not_consult_compatibility_projection);
	UT_RUN(test_create_concurrently_refuses_before_native_mutation);
	UT_RUN(test_reindex_kinds_and_false_option);
	UT_RUN(test_shared_index_access_methods_refuse_before_utility_or_define);
	UT_RUN(test_nonbtree_constraints_rejected_before_create_or_alter_table);
	UT_RUN(test_schema_checks_nested_index_before_namespace_creation);
	UT_RUN(test_native_index_create_requires_builtin_btree_oid);
	UT_RUN(test_primary_unique_and_nonindex_constraints_keep_native_admission);
	UT_RUN(test_shared_removed_commands_refuse_at_native_utility_entry);
	UT_RUN(test_unlogged_and_matview_create_and_alter_refuse_before_work);
	UT_RUN(test_schema_prechecks_removed_children_but_retains_supported_commands);
	UT_RUN(test_shared_selected_control_requires_supported_checksums);
	UT_RUN(test_shared_unbound_checksum_refusal_precedes_root_and_writes);
	UT_RUN(test_nonshared_control_keeps_native_checksum_choice);
	UT_RUN(test_shared_two_phase_refuses_but_transaction_control_remains_native);
	UT_RUN(test_rdma_drop_checks_native_targets_before_later_utility_work);
	UT_RUN(test_shared_smgr_drop_preflight_is_independent_of_shared_catalog_gucs);
	UT_RUN(test_shared_create_binds_before_later_utility_work_and_reuses_top);
	UT_RUN(test_create_publication_refusal_precedes_later_utility_work);
	UT_RUN(test_create_rejects_child_or_wrong_top_before_xid_and_binding);
	UT_RUN(test_create_preflight_only_targets_shared_catalog_storage);
	UT_RUN(test_create_original_readonly_and_parallel_guards_precede_binding);
	UT_RUN(test_unsupported_creates_refuse_before_xid_or_binding);
	UT_RUN(test_noncreate_utility_does_not_reserve_canonical_binding);
	UT_RUN(test_unsupported_statistics_from_keeps_native_path_without_binding);
	UT_DONE();
	return ut_failed_count != 0;
}
