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
#include "access/genam.h"
#include "access/heaptoast.h"
#include "access/reloptions.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/catversion.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_control.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_policy.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_transform.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_user_mapping.h"
#include "commands/alter.h"
#include "commands/collationcmds.h"
#include "commands/comment.h"
#include "commands/conversioncmds.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/event_trigger.h"
#include "commands/policy.h"
#include "commands/proclang.h"
#include "commands/publicationcmds.h"
#include "commands/schemacmds.h"
#include "commands/seclabel.h"
#include "commands/sequence.h"
#include "commands/user.h"
#include "commands/view.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_tt_local.h"
#include "commands/tablecmds.h"
#include "common/controldata_utils.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/value.h"
#include "parser/parse_node.h"
#include "parser/analyze.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "rewrite/rewriteDefine.h"
#include "port/pg_crc32c.h"
#include "statistics/statistics.h"
#include "storage/fd.h"
#include "storage/large_object.h"
#include "storage/lock.h"
#include "tcop/utility.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/aclchk_internal.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
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
static char error_detail[160];
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
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(error_detail, sizeof(error_detail), fmt, ap);
	va_end(ap);
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
static bool utility_recovery;
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
	return utility_recovery;
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

/* Native rename scopes preserve all existing tag and lookup fallbacks. */
static bool rename_dispatch_lookup_active, rename_dispatch_expect_binding;
static unsigned rename_dispatch_lookups, rename_dispatch_lookup_kind,
	rename_dispatch_expected_boundary;
static unsigned rename_dispatch_unbound, rename_dispatch_bad_lookup,
	rename_dispatch_unexpected_dependencies;
static RenameStmt native_rename_dispatch_statement;
static Node *rename_dispatch_expected_statement;
#include "test_cluster_fresh_rename_tag.inc"
static const char *const rename_dispatch_command_names[] = {
#define PG_CMDTAG(symbol, name, event, rewrite, rows) [symbol] = name,
#include "tcop/cmdtaglist.h"
#undef PG_CMDTAG
};

/* Native SET SCHEMA reuses the real object-tag helper and name table. */
static bool schema_dispatch_lookup_active, schema_dispatch_expect_binding;
static unsigned schema_dispatch_lookups, schema_dispatch_lookup_kind,
	schema_dispatch_expected_boundary;
static unsigned schema_dispatch_unbound, schema_dispatch_bad_lookup,
	schema_dispatch_unexpected_dependencies;
static AlterObjectSchemaStmt native_schema_dispatch_statement;
static Node *schema_dispatch_expected_statement;
static ObjectAddress schema_dispatch_old_schema_address;
static bool schema_dispatch_wants_old_schema;

/* Native database identity entries reuse the actual tag/name dependencies. */
static bool database_identity_lookup_active, database_identity_expect_binding;
static unsigned database_identity_lookups, database_identity_unbound, database_identity_bad_lookup;

CommandTag
CreateCommandTag(Node *statement)
{
	if (database_identity_lookup_active
		&& (IsA(statement, RenameStmt) || IsA(statement, AlterOwnerStmt))) {
		ObjectType type = IsA(statement, RenameStmt) ? ((RenameStmt *)statement)->renameType
													 : ((AlterOwnerStmt *)statement)->objectType;
		return AlterObjectTypeCommandTag(type);
	}
	if (schema_dispatch_lookup_active && IsA(statement, AlterObjectSchemaStmt))
		return AlterObjectTypeCommandTag(((AlterObjectSchemaStmt *)statement)->objectType);
	if (rename_dispatch_lookup_active && IsA(statement, RenameStmt)) {
		RenameStmt *stmt = (RenameStmt *)statement;
		return AlterObjectTypeCommandTag(stmt->renameType == OBJECT_COLUMN ? stmt->relationType
																		   : stmt->renameType);
	}
	if (IsA(statement, TruncateStmt))
		return CMDTAG_TRUNCATE_TABLE;
	if (IsA(statement, CreateForeignTableStmt))
		return CMDTAG_CREATE_FOREIGN_TABLE;
	if (IsA(statement, ImportForeignSchemaStmt))
		return CMDTAG_IMPORT_FOREIGN_SCHEMA;
	if (IsA(statement, AlterExtensionContentsStmt) || IsA(statement, AlterExtensionStmt))
		return CMDTAG_ALTER_EXTENSION;
	if (IsA(statement, CreateSchemaStmt))
		return CMDTAG_CREATE_SCHEMA;
	if (IsA(statement, CreateSeqStmt))
		return CMDTAG_CREATE_SEQUENCE;
	if (IsA(statement, IndexStmt))
		return CMDTAG_CREATE_INDEX;
	if (IsA(statement, ViewStmt))
		return CMDTAG_CREATE_VIEW;
	if (IsA(statement, AlterTableStmt))
		return CMDTAG_ALTER_TABLE;
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
	if (IsA(statement, AlterOwnerStmt)) {
		switch (((AlterOwnerStmt *)statement)->objectType) {
		case OBJECT_DATABASE:
			return CMDTAG_ALTER_DATABASE;
		case OBJECT_SCHEMA:
			return CMDTAG_ALTER_SCHEMA;
		case OBJECT_FUNCTION:
			return CMDTAG_ALTER_FUNCTION;
		case OBJECT_COLLATION:
			return CMDTAG_ALTER_COLLATION;
		case OBJECT_EVENT_TRIGGER:
			return CMDTAG_ALTER_EVENT_TRIGGER;
		case OBJECT_TABLESPACE:
			return CMDTAG_ALTER_TABLESPACE;
		case OBJECT_AGGREGATE:
			return CMDTAG_ALTER_AGGREGATE;
		case OBJECT_CONVERSION:
			return CMDTAG_ALTER_CONVERSION;
		case OBJECT_FDW:
			return CMDTAG_ALTER_FOREIGN_DATA_WRAPPER;
		case OBJECT_FOREIGN_SERVER:
			return CMDTAG_ALTER_SERVER;
		case OBJECT_LANGUAGE:
			return CMDTAG_ALTER_LANGUAGE;
		case OBJECT_LARGEOBJECT:
			return CMDTAG_ALTER_LARGE_OBJECT;
		case OBJECT_PROCEDURE:
			return CMDTAG_ALTER_PROCEDURE;
		case OBJECT_PUBLICATION:
			return CMDTAG_ALTER_PUBLICATION;
		case OBJECT_ROUTINE:
			return CMDTAG_ALTER_ROUTINE;
		case OBJECT_STATISTIC_EXT:
			return CMDTAG_ALTER_STATISTICS;
		case OBJECT_SUBSCRIPTION:
			return CMDTAG_ALTER_SUBSCRIPTION;
		case OBJECT_TSCONFIGURATION:
			return CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION;
		case OBJECT_TSDICTIONARY:
			return CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY;
		default:
			return CMDTAG_UNKNOWN;
		}
	}
	if (IsA(statement, AlterObjectDependsStmt)) {
		switch (((AlterObjectDependsStmt *)statement)->objectType) {
		case OBJECT_FUNCTION:
			return CMDTAG_ALTER_FUNCTION;
		case OBJECT_PROCEDURE:
			return CMDTAG_ALTER_PROCEDURE;
		case OBJECT_ROUTINE:
			return CMDTAG_ALTER_ROUTINE;
		case OBJECT_TRIGGER:
			return CMDTAG_ALTER_TRIGGER;
		case OBJECT_INDEX:
			return CMDTAG_ALTER_INDEX;
		case OBJECT_MATVIEW:
			return CMDTAG_ALTER_MATERIALIZED_VIEW;
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
	if (IsA(statement, SecLabelStmt))
		return CMDTAG_SECURITY_LABEL;
	if (IsA(statement, CreateRoleStmt))
		return CMDTAG_CREATE_ROLE;
	if (IsA(statement, AlterRoleStmt) || IsA(statement, AlterRoleSetStmt))
		return CMDTAG_ALTER_ROLE;
	if (IsA(statement, DropRoleStmt))
		return CMDTAG_DROP_ROLE;
	if (IsA(statement, AlterTableSpaceOptionsStmt))
		return CMDTAG_ALTER_TABLESPACE;
	if (IsA(statement, ReassignOwnedStmt))
		return CMDTAG_REASSIGN_OWNED;
	if (IsA(statement, DropOwnedStmt))
		return CMDTAG_DROP_OWNED;
	if (IsA(statement, DropStmt)) {
		switch (((DropStmt *)statement)->removeType) {
		case OBJECT_TABLE:
			return CMDTAG_DROP_TABLE;
		case OBJECT_INDEX:
			return CMDTAG_DROP_INDEX;
		case OBJECT_SEQUENCE:
			return CMDTAG_DROP_SEQUENCE;
		case OBJECT_VIEW:
			return CMDTAG_DROP_VIEW;
		case OBJECT_FOREIGN_TABLE:
			return CMDTAG_DROP_FOREIGN_TABLE;
		case OBJECT_SCHEMA:
			return CMDTAG_DROP_SCHEMA;
		case OBJECT_COLLATION:
			return CMDTAG_DROP_COLLATION;
		default:
			return CMDTAG_UNKNOWN;
		}
	}
	if (IsA(statement, AlterDatabaseSetStmt) || IsA(statement, AlterDatabaseRefreshCollStmt)
		|| IsA(statement, AlterDatabaseStmt))
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
	if (IsA(statement, CreateFdwStmt))
		return CMDTAG_CREATE_FOREIGN_DATA_WRAPPER;
	if (IsA(statement, AlterFdwStmt))
		return CMDTAG_ALTER_FOREIGN_DATA_WRAPPER;
	if (IsA(statement, CreateForeignServerStmt))
		return CMDTAG_CREATE_SERVER;
	if (IsA(statement, AlterForeignServerStmt))
		return CMDTAG_ALTER_SERVER;
	if (IsA(statement, CreateUserMappingStmt))
		return CMDTAG_CREATE_USER_MAPPING;
	if (IsA(statement, AlterUserMappingStmt))
		return CMDTAG_ALTER_USER_MAPPING;
	if (IsA(statement, DropUserMappingStmt))
		return CMDTAG_DROP_USER_MAPPING;
	if (IsA(statement, CreatePublicationStmt))
		return CMDTAG_CREATE_PUBLICATION;
	if (IsA(statement, AlterPublicationStmt))
		return CMDTAG_ALTER_PUBLICATION;
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
	if (database_identity_lookup_active) {
		if ((unsigned)tag >= lengthof(rename_dispatch_command_names))
			siglongjmp(boundary, 3);
		return rename_dispatch_command_names[tag];
	}
	if (schema_dispatch_lookup_active) {
		if ((unsigned)tag >= lengthof(rename_dispatch_command_names))
			siglongjmp(boundary, 3);
		return rename_dispatch_command_names[tag];
	}
	if (rename_dispatch_lookup_active) {
		if ((unsigned)tag >= lengthof(rename_dispatch_command_names))
			siglongjmp(boundary, 3);
		return rename_dispatch_command_names[tag];
	}
	if (tag == CMDTAG_ALTER_AGGREGATE)
		return "ALTER AGGREGATE";
	if (tag == CMDTAG_ALTER_CONVERSION)
		return "ALTER CONVERSION";
	if (tag == CMDTAG_ALTER_LANGUAGE)
		return "ALTER LANGUAGE";
	if (tag == CMDTAG_ALTER_LARGE_OBJECT)
		return "ALTER LARGE OBJECT";
	if (tag == CMDTAG_ALTER_SUBSCRIPTION)
		return "ALTER SUBSCRIPTION";
	switch (tag) {
	case CMDTAG_DROP_TABLE:
		return "DROP TABLE";
	case CMDTAG_DROP_INDEX:
		return "DROP INDEX";
	case CMDTAG_DROP_SEQUENCE:
		return "DROP SEQUENCE";
	case CMDTAG_DROP_VIEW:
		return "DROP VIEW";
	case CMDTAG_DROP_FOREIGN_TABLE:
		return "DROP FOREIGN TABLE";
	case CMDTAG_DROP_SCHEMA:
		return "DROP SCHEMA";
	case CMDTAG_DROP_COLLATION:
		return "DROP COLLATION";
	default:
		break;
	}
	if (tag == CMDTAG_TRUNCATE_TABLE)
		return "TRUNCATE TABLE";
	if (tag == CMDTAG_CREATE_FOREIGN_TABLE)
		return "CREATE FOREIGN TABLE";
	if (tag == CMDTAG_IMPORT_FOREIGN_SCHEMA)
		return "IMPORT FOREIGN SCHEMA";
	if (tag == CMDTAG_ALTER_EXTENSION)
		return "ALTER EXTENSION";
	if (tag == CMDTAG_CREATE_SCHEMA)
		return "CREATE SCHEMA";
	if (tag == CMDTAG_CREATE_SEQUENCE)
		return "CREATE SEQUENCE";
	if (tag == CMDTAG_CREATE_INDEX)
		return "CREATE INDEX";
	if (tag == CMDTAG_CREATE_VIEW)
		return "CREATE VIEW";
	if (tag == CMDTAG_ALTER_SCHEMA)
		return "ALTER SCHEMA";
	if (tag == CMDTAG_ALTER_TABLESPACE)
		return "ALTER TABLESPACE";
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
	if (tag == CMDTAG_ALTER_TRIGGER)
		return "ALTER TRIGGER";
	if (tag == CMDTAG_ALTER_INDEX)
		return "ALTER INDEX";
	if (tag == CMDTAG_ALTER_MATERIALIZED_VIEW)
		return "ALTER MATERIALIZED VIEW";
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
	if (tag == CMDTAG_SECURITY_LABEL)
		return "SECURITY LABEL";
	if (tag == CMDTAG_CREATE_ROLE)
		return "CREATE ROLE";
	if (tag == CMDTAG_ALTER_ROLE)
		return "ALTER ROLE";
	if (tag == CMDTAG_DROP_ROLE)
		return "DROP ROLE";
	if (tag == CMDTAG_REASSIGN_OWNED)
		return "REASSIGN OWNED";
	if (tag == CMDTAG_DROP_OWNED)
		return "DROP OWNED";
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
	if (tag == CMDTAG_CREATE_FOREIGN_DATA_WRAPPER)
		return "CREATE FOREIGN DATA WRAPPER";
	if (tag == CMDTAG_ALTER_FOREIGN_DATA_WRAPPER)
		return "ALTER FOREIGN DATA WRAPPER";
	if (tag == CMDTAG_CREATE_SERVER)
		return "CREATE SERVER";
	if (tag == CMDTAG_ALTER_SERVER)
		return "ALTER SERVER";
	if (tag == CMDTAG_CREATE_USER_MAPPING)
		return "CREATE USER MAPPING";
	if (tag == CMDTAG_ALTER_USER_MAPPING)
		return "ALTER USER MAPPING";
	if (tag == CMDTAG_DROP_USER_MAPPING)
		return "DROP USER MAPPING";
	if (tag == CMDTAG_CREATE_PUBLICATION)
		return "CREATE PUBLICATION";
	if (tag == CMDTAG_ALTER_PUBLICATION)
		return "ALTER PUBLICATION";
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
		error_code = utility_in_transaction || !isTopLevel ? ERRCODE_ACTIVE_SQL_TRANSACTION
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
	error_detail[0] = '\0';
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
	utility_recovery = false;
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
		UT_ASSERT_EQ(utility_binds, 0);
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
	AlterOwnerStmt owner = { .type = T_AlterOwnerStmt };
	Node *statements[] = { (Node *)&rename, (Node *)&move, (Node *)&owner };
	const RenameStmt variants[]
		= { { .type = T_RenameStmt, .renameType = OBJECT_COLUMN, .relationType = OBJECT_MATVIEW },
			{ .type = T_RenameStmt, .renameType = OBJECT_ATTRIBUTE, .relationType = OBJECT_TYPE },
			{ .type = T_RenameStmt, .renameType = OBJECT_DOMCONSTRAINT } };

	for (unsigned i = 0; i < lengthof(types); i++) {
		rename.renameType = move.objectType = owner.objectType = types[i];
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
	DefElem no_validator = { .type = T_DefElem, .defname = "validator" };
	ListCell validator_cell = { .ptr_value = &no_validator };
	List validator_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &validator_cell };
	CreateFdwStmt create_fdw
		= { .type = T_CreateFdwStmt, .fdwname = "target_fdw", .func_options = &validator_options };
	AlterFdwStmt alter_fdw
		= { .type = T_AlterFdwStmt, .fdwname = "target_fdw", .func_options = &validator_options };
	CreateForeignServerStmt create_server = { .type = T_CreateForeignServerStmt,
											  .servername = "target_server",
											  .fdwname = "target_fdw" };
	AlterForeignServerStmt alter_server = { .type = T_AlterForeignServerStmt,
											.servername = "target_server",
											.version = "preflight",
											.has_version = true };
	RoleSpec mapping_user = { .type = T_RoleSpec, .roletype = ROLESPEC_CURRENT_USER };
	CreateUserMappingStmt create_mapping
		= { .type = T_CreateUserMappingStmt, .user = &mapping_user, .servername = "target_server" };
	String mapping_option_value = { .type = T_String, .sval = "enabled" };
	DefElem mapping_option = { .type = T_DefElem,
							   .defname = "g_preflight",
							   .arg = (Node *)&mapping_option_value,
							   .defaction = DEFELEM_ADD };
	ListCell mapping_cell = { .ptr_value = &mapping_option };
	List mapping_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &mapping_cell };
	AlterUserMappingStmt alter_mapping = { .type = T_AlterUserMappingStmt,
										   .user = &mapping_user,
										   .servername = "target_server",
										   .options = &mapping_options };
	DropUserMappingStmt drop_mapping
		= { .type = T_DropUserMappingStmt, .user = &mapping_user, .servername = "target_server" };
	String publish_value = { .type = T_String, .sval = "insert" };
	DefElem publish_option
		= { .type = T_DefElem, .defname = "publish", .arg = (Node *)&publish_value };
	ListCell publish_option_cell = { .ptr_value = &publish_option };
	List publish_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &publish_option_cell };
	PublicationTable publication_table
		= { .type = T_PublicationTable, .relation = &stats_relation };
	PublicationObjSpec publication_table_spec = { .type = T_PublicationObjSpec,
												  .pubobjtype = PUBLICATIONOBJ_TABLE,
												  .pubtable = &publication_table };
	PublicationObjSpec publication_schema_spec = { .type = T_PublicationObjSpec,
												   .pubobjtype = PUBLICATIONOBJ_TABLES_IN_SCHEMA,
												   .name = "target_schema" };
	ListCell publication_cells[2]
		= { { .ptr_value = &publication_table_spec }, { .ptr_value = &publication_schema_spec } };
	List publication_objects
		= { .type = T_List, .length = 2, .max_length = 2, .elements = publication_cells };
	CreatePublicationStmt create_publication = { .type = T_CreatePublicationStmt,
												 .pubname = "target_publication",
												 .options = &publish_options };
	CreatePublicationStmt create_publication_objects = { .type = T_CreatePublicationStmt,
														 .pubname = "target_publication",
														 .pubobjects = &publication_objects };
	AlterPublicationStmt alter_publication_options = { .type = T_AlterPublicationStmt,
													   .pubname = "target_publication",
													   .options = &publish_options };
	AlterPublicationStmt publication_add = { .type = T_AlterPublicationStmt,
											 .pubname = "target_publication",
											 .pubobjects = &publication_objects,
											 .action = AP_AddObjects };
	AlterPublicationStmt publication_drop = { .type = T_AlterPublicationStmt,
											  .pubname = "target_publication",
											  .pubobjects = &publication_objects,
											  .action = AP_DropObjects };
	AlterPublicationStmt publication_set = { .type = T_AlterPublicationStmt,
											 .pubname = "target_publication",
											 .pubobjects = &publication_objects,
											 .action = AP_SetObjects };
	String dependency_name = { .type = T_String, .sval = "target_object" };
	ListCell dependency_name_cell = { .ptr_value = &dependency_name };
	List dependency_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &dependency_name_cell };
	ObjectWithArgs dependency_routine = { .type = T_ObjectWithArgs, .objname = &dependency_names };
	String dependency_extension = { .type = T_String, .sval = "target_extension" };
	AlterObjectDependsStmt depends_function = { .type = T_AlterObjectDependsStmt,
												.objectType = OBJECT_FUNCTION,
												.object = (Node *)&dependency_routine,
												.extname = &dependency_extension };
	AlterObjectDependsStmt depends_procedure = { .type = T_AlterObjectDependsStmt,
												 .objectType = OBJECT_PROCEDURE,
												 .object = (Node *)&dependency_routine,
												 .extname = &dependency_extension };
	AlterObjectDependsStmt depends_routine = { .type = T_AlterObjectDependsStmt,
											   .objectType = OBJECT_ROUTINE,
											   .object = (Node *)&dependency_routine,
											   .extname = &dependency_extension };
	AlterObjectDependsStmt depends_trigger = { .type = T_AlterObjectDependsStmt,
											   .objectType = OBJECT_TRIGGER,
											   .relation = &stats_relation,
											   .object = (Node *)&dependency_names,
											   .extname = &dependency_extension };
	AlterObjectDependsStmt depends_index = { .type = T_AlterObjectDependsStmt,
											 .objectType = OBJECT_INDEX,
											 .relation = &stats_relation,
											 .extname = &dependency_extension };
	RoleSpec reassign_first
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "source_owner" };
	RoleSpec reassign_second
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "second_owner" };
	RoleSpec reassign_receiver
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	ListCell reassign_cells[2]
		= { { .ptr_value = &reassign_first }, { .ptr_value = &reassign_second } };
	List reassign_roles
		= { .type = T_List, .length = 2, .max_length = 2, .elements = reassign_cells };
	ReassignOwnedStmt reassign_named
		= { .type = T_ReassignOwnedStmt, .roles = &reassign_roles, .newrole = &reassign_receiver };
	RoleSpec reassign_self = { .type = T_RoleSpec, .roletype = ROLESPEC_CURRENT_USER };
	ListCell reassign_self_cell = { .ptr_value = &reassign_self };
	List reassign_self_roles
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &reassign_self_cell };
	ReassignOwnedStmt reassign_context
		= { .type = T_ReassignOwnedStmt, .roles = &reassign_self_roles, .newrole = &reassign_self };
	RangeVar foreign_relation = { .type = T_RangeVar,
								  .schemaname = "g_preflight",
								  .relname = "foreign_target",
								  .relpersistence = RELPERSISTENCE_PERMANENT };
	String foreign_option_value = { .type = T_String, .sval = "preflight" };
	DefElem foreign_option
		= { .type = T_DefElem, .defname = "g_preflight", .arg = (Node *)&foreign_option_value };
	ListCell foreign_option_cell = { .ptr_value = &foreign_option };
	List foreign_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &foreign_option_cell };
	CreateForeignTableStmt foreign_table = { .base = { .type = T_CreateForeignTableStmt,
													   .relation = &foreign_relation,
													   .oncommit = ONCOMMIT_NOOP },
											 .servername = "target_server",
											 .options = &foreign_options };
	RoleSpec owner_receiver
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	String owner_target = { .type = T_String, .sval = "owner_target" };
	String owner_schema_name = { .type = T_String, .sval = "pg_undo" };
	String owner_space_name = { .type = T_String, .sval = "g_preflight_tablespace" };
	AlterOwnerStmt owner_database = { .type = T_AlterOwnerStmt,
									  .objectType = OBJECT_DATABASE,
									  .object = (Node *)&owner_target,
									  .newowner = &owner_receiver };
	AlterOwnerStmt owner_schema = { .type = T_AlterOwnerStmt,
									.objectType = OBJECT_SCHEMA,
									.object = (Node *)&owner_schema_name,
									.newowner = &reassign_self };
	AlterOwnerStmt owner_function = { .type = T_AlterOwnerStmt,
									  .objectType = OBJECT_FUNCTION,
									  .object = (Node *)&dependency_routine,
									  .newowner = &owner_receiver };
	AlterOwnerStmt owner_collation = { .type = T_AlterOwnerStmt,
									   .objectType = OBJECT_COLLATION,
									   .object = (Node *)&dependency_names,
									   .newowner = &owner_receiver };
	AlterOwnerStmt owner_tablespace = { .type = T_AlterOwnerStmt,
										.objectType = OBJECT_TABLESPACE,
										.object = (Node *)&owner_space_name,
										.newowner = &owner_receiver };
	String database_template_value = { .type = T_String, .sval = "false" };
	String database_allow_value = { .type = T_String, .sval = "true" };
	Integer database_limit_value = { .type = T_Integer, .ival = 100 };
	DefElem database_template_option
		= { .type = T_DefElem, .defname = "is_template", .arg = (Node *)&database_template_value };
	DefElem database_allow_option = { .type = T_DefElem,
									  .defname = "allow_connections",
									  .arg = (Node *)&database_allow_value };
	DefElem database_limit_option = { .type = T_DefElem,
									  .defname = "connection_limit",
									  .arg = (Node *)&database_limit_value };
	ListCell database_flags_cells[2]
		= { { .ptr_value = &database_template_option }, { .ptr_value = &database_allow_option } };
	List database_flags_options
		= { .type = T_List, .length = 2, .max_length = 2, .elements = database_flags_cells };
	ListCell database_limit_cell = { .ptr_value = &database_limit_option };
	List database_limit_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &database_limit_cell };
	AlterDatabaseStmt database_flags = { .type = T_AlterDatabaseStmt,
										 .dbname = "target_database",
										 .options = &database_flags_options };
	AlterDatabaseStmt database_limit = { .type = T_AlterDatabaseStmt,
										 .dbname = "target_database",
										 .options = &database_limit_options };
	Integer tablespace_cost_value = { .type = T_Integer, .ival = 1 };
	DefElem tablespace_option
		= { .type = T_DefElem, .defname = "seq_page_cost", .arg = (Node *)&tablespace_cost_value };
	ListCell tablespace_option_cell = { .ptr_value = &tablespace_option };
	List tablespace_option_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &tablespace_option_cell };
	AlterTableSpaceOptionsStmt tablespace_options = { .type = T_AlterTableSpaceOptionsStmt,
													  .tablespacename = "g_preflight_tablespace",
													  .options = &tablespace_option_list };
	String label_name = { .type = T_String, .sval = "label_target" };
	String label_column_name = { .type = T_String, .sval = "label_column" };
	ListCell label_name_cell = { .ptr_value = &label_name };
	List label_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &label_name_cell };
	ListCell label_column_cells[2]
		= { { .ptr_value = &label_name }, { .ptr_value = &label_column_name } };
	List label_column_names
		= { .type = T_List, .length = 2, .max_length = 2, .elements = label_column_cells };
	ObjectWithArgs label_routine = { .type = T_ObjectWithArgs, .objname = &label_names };
	SecLabelStmt label_table = { .type = T_SecLabelStmt,
								 .objtype = OBJECT_TABLE,
								 .object = (Node *)&label_names,
								 .provider = "g_label_provider",
								 .label = "table_label" };
	SecLabelStmt label_column = { .type = T_SecLabelStmt,
								  .objtype = OBJECT_COLUMN,
								  .object = (Node *)&label_column_names };
	SecLabelStmt label_function = { .type = T_SecLabelStmt,
									.objtype = OBJECT_FUNCTION,
									.object = (Node *)&label_routine,
									.provider = "g_label_provider",
									.label = "routine_label" };
	SecLabelStmt label_database
		= { .type = T_SecLabelStmt, .objtype = OBJECT_DATABASE, .object = (Node *)&label_name };
	RangeVar static_relation = { .type = T_RangeVar,
								 .relname = "alter_target",
								 .inh = true,
								 .relpersistence = RELPERSISTENCE_PERMANENT };
	Integer static_stats_value = { .type = T_Integer, .ival = 100 };
	AlterTableCmd static_stats_cmd = { .type = T_AlterTableCmd,
									   .subtype = AT_SetStatistics,
									   .name = "target_column",
									   .def = (Node *)&static_stats_value };
	RoleSpec static_owner_role
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	AlterTableCmd static_owner_cmd
		= { .type = T_AlterTableCmd, .subtype = AT_ChangeOwner, .newowner = &static_owner_role };
	Integer static_fillfactor = { .type = T_Integer, .ival = 90 };
	DefElem static_set_option
		= { .type = T_DefElem, .defname = "fillfactor", .arg = (Node *)&static_fillfactor };
	DefElem static_reset_option = { .type = T_DefElem, .defname = "autovacuum_enabled" };
	ListCell static_set_cell = { .ptr_value = &static_set_option };
	ListCell static_reset_cell = { .ptr_value = &static_reset_option };
	List static_set_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_set_cell };
	List static_reset_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_reset_cell };
	AlterTableCmd static_set_cmd
		= { .type = T_AlterTableCmd, .subtype = AT_SetRelOptions, .def = (Node *)&static_set_list };
	AlterTableCmd static_reset_cmd = { .type = T_AlterTableCmd,
									   .subtype = AT_ResetRelOptions,
									   .def = (Node *)&static_reset_list };
	ListCell static_stats_cell = { .ptr_value = &static_stats_cmd };
	ListCell static_owner_cell = { .ptr_value = &static_owner_cmd };
	ListCell static_option_cells[2]
		= { { .ptr_value = &static_set_cmd }, { .ptr_value = &static_reset_cmd } };
	List static_stats_cmds
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_stats_cell };
	List static_owner_cmds
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_owner_cell };
	List static_option_cmds
		= { .type = T_List, .length = 2, .max_length = 2, .elements = static_option_cells };
	AlterTableStmt static_statistics = { .type = T_AlterTableStmt,
										 .objtype = OBJECT_TABLE,
										 .relation = &static_relation,
										 .cmds = &static_stats_cmds };
	AlterTableStmt static_owner = { .type = T_AlterTableStmt,
									.objtype = OBJECT_TABLE,
									.relation = &static_relation,
									.cmds = &static_owner_cmds };
	AlterTableStmt static_options = { .type = T_AlterTableStmt,
									  .objtype = OBJECT_TABLE,
									  .relation = &static_relation,
									  .cmds = &static_option_cmds };
	RangeVar import_first = { .type = T_RangeVar, .relname = "import_first", .location = -1 };
	RangeVar import_second = { .type = T_RangeVar, .relname = "import_second", .location = -1 };
	ListCell import_table_cells[2]
		= { { .ptr_value = &import_first }, { .ptr_value = &import_second } };
	List import_tables
		= { .type = T_List, .length = 2, .max_length = 2, .elements = import_table_cells };
	String import_option_value = { .type = T_String, .sval = "true" };
	DefElem import_option
		= { .type = T_DefElem, .defname = "import_default", .arg = (Node *)&import_option_value };
	ListCell import_option_cell = { .ptr_value = &import_option };
	List import_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &import_option_cell };
	ImportForeignSchemaStmt import_all = { .type = T_ImportForeignSchemaStmt,
										   .server_name = "target_server",
										   .remote_schema = "remote_schema",
										   .local_schema = "local_schema",
										   .list_type = FDW_IMPORT_SCHEMA_ALL };
	ImportForeignSchemaStmt import_limit = { .type = T_ImportForeignSchemaStmt,
											 .server_name = "target_server",
											 .remote_schema = "remote_schema",
											 .local_schema = "local_schema",
											 .list_type = FDW_IMPORT_SCHEMA_LIMIT_TO,
											 .table_list = &import_tables,
											 .options = &import_options };
	ImportForeignSchemaStmt import_except = { .type = T_ImportForeignSchemaStmt,
											  .server_name = "target_server",
											  .remote_schema = "remote_schema",
											  .local_schema = "local_schema",
											  .list_type = FDW_IMPORT_SCHEMA_EXCEPT,
											  .table_list = &import_tables,
											  .options = &import_options };
	AlterExtensionContentsStmt extension_add_table = { .type = T_AlterExtensionContentsStmt,
													   .extname = "existing_extension",
													   .action = 1,
													   .objtype = OBJECT_TABLE,
													   .object = (Node *)&dependency_names };
	AlterExtensionContentsStmt extension_drop_table = { .type = T_AlterExtensionContentsStmt,
														.extname = "existing_extension",
														.action = -1,
														.objtype = OBJECT_TABLE,
														.object = (Node *)&dependency_names };
	AlterExtensionContentsStmt extension_add_function = { .type = T_AlterExtensionContentsStmt,
														  .extname = "existing_extension",
														  .action = 1,
														  .objtype = OBJECT_FUNCTION,
														  .object = (Node *)&dependency_routine };
	AlterExtensionContentsStmt extension_drop_function = { .type = T_AlterExtensionContentsStmt,
														   .extname = "existing_extension",
														   .action = -1,
														   .objtype = OBJECT_FUNCTION,
														   .object = (Node *)&dependency_routine };
	String extension_version = { .type = T_String, .sval = "2.0" };
	DefElem version_option
		= { .type = T_DefElem, .defname = "new_version", .arg = (Node *)&extension_version };
	ListCell version_cell = { .ptr_value = &version_option };
	List version_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &version_cell };
	AlterExtensionStmt extension_default
		= { .type = T_AlterExtensionStmt, .extname = "existing_extension" };
	AlterExtensionStmt extension_explicit = { .type = T_AlterExtensionStmt,
											  .extname = "existing_extension",
											  .options = &version_options };
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
						   (Node *)&access_method,
						   (Node *)&create_fdw,
						   (Node *)&alter_fdw,
						   (Node *)&create_server,
						   (Node *)&alter_server,
						   (Node *)&create_mapping,
						   (Node *)&alter_mapping,
						   (Node *)&drop_mapping,
						   (Node *)&create_publication,
						   (Node *)&create_publication_objects,
						   (Node *)&alter_publication_options,
						   (Node *)&publication_add,
						   (Node *)&publication_drop,
						   (Node *)&publication_set,
						   (Node *)&depends_function,
						   (Node *)&depends_procedure,
						   (Node *)&depends_routine,
						   (Node *)&depends_trigger,
						   (Node *)&depends_index,
						   (Node *)&reassign_named,
						   (Node *)&reassign_context,
						   (Node *)&foreign_table,
						   (Node *)&owner_database,
						   (Node *)&owner_schema,
						   (Node *)&owner_function,
						   (Node *)&owner_collation,
						   (Node *)&owner_tablespace,
						   (Node *)&database_flags,
						   (Node *)&database_limit,
						   (Node *)&tablespace_options,
						   (Node *)&label_table,
						   (Node *)&label_column,
						   (Node *)&label_function,
						   (Node *)&label_database,
						   (Node *)&static_statistics,
						   (Node *)&static_owner,
						   (Node *)&static_options,
						   (Node *)&import_all,
						   (Node *)&import_limit,
						   (Node *)&import_except,
						   (Node *)&extension_add_table,
						   (Node *)&extension_drop_table,
						   (Node *)&extension_add_function,
						   (Node *)&extension_drop_function,
						   (Node *)&extension_default,
						   (Node *)&extension_explicit };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		for (unsigned skip = 0; skip < 2; skip++) {
			reset_create_preflight();
			utility_expect_binding = true;
			import_option_value.sval = skip ? "false" : "true";
			import_all.options = skip ? &import_options : NIL;
			static_stats_value.ival = skip ? -1 : 100;
			static_fillfactor.ival = skip ? 80 : 90;
			static_owner_role.roletype = skip ? ROLESPEC_CURRENT_USER : ROLESPEC_CSTRING;
			static_owner_role.rolename = skip ? NULL : "receiving_owner";
			static_statistics.missing_ok = static_owner.missing_ok = static_options.missing_ok
				= skip != 0;
			static_relation.inh = skip == 0;
			label_table.label = skip ? NULL : "table_label";
			label_column.label = skip ? "column_label" : NULL;
			label_function.label = skip ? NULL : "routine_label";
			label_database.label = skip ? "database_label" : NULL;
			label_table.provider = label_function.provider = skip ? NULL : "g_label_provider";
			label_column.provider = label_database.provider = skip ? "g_label_provider" : NULL;
			database_template_value.sval = skip ? "true" : "false";
			database_allow_value.sval = skip ? "false" : "true";
			database_limit_value.ival = skip ? -1 : 100;
			tablespace_options.isReset = skip != 0;
			tablespace_options.tablespacename = skip ? "PG_UNDO" : "g_preflight_tablespace";
			tablespace_option.arg = skip ? NULL : (Node *)&tablespace_cost_value;
			owner_space_name.sval = skip ? "PG_UNDO" : "g_preflight_tablespace";
			owner_tablespace.newowner = skip ? &reassign_self : &owner_receiver;
			foreign_table.base.if_not_exists = skip != 0;
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
			create_server.if_not_exists = create_mapping.if_not_exists = drop_mapping.missing_ok
				= skip != 0;
			alter_server.version = skip ? NULL : "preflight";
			mapping_user.roletype = skip ? ROLESPEC_PUBLIC : ROLESPEC_CURRENT_USER;
			mapping_option.defaction = skip ? DEFELEM_SET : DEFELEM_ADD;
			create_publication.for_all_tables = skip != 0;
			publish_value.sval = skip ? "insert, update" : "insert";
			depends_function.remove = depends_procedure.remove = depends_routine.remove
				= depends_trigger.remove = depends_index.remove = skip != 0;
			reassign_named.newrole = skip ? &reassign_first : &reassign_receiver;
			reassign_self.roletype = skip ? ROLESPEC_SESSION_USER : ROLESPEC_CURRENT_USER;
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
	DefElem no_validator = { .type = T_DefElem, .defname = "validator" };
	ListCell validator_cell = { .ptr_value = &no_validator };
	List validator_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &validator_cell };
	CreateFdwStmt create_fdw
		= { .type = T_CreateFdwStmt, .fdwname = "target_fdw", .func_options = &validator_options };
	AlterFdwStmt alter_fdw
		= { .type = T_AlterFdwStmt, .fdwname = "target_fdw", .func_options = &validator_options };
	CreateForeignServerStmt create_server = { .type = T_CreateForeignServerStmt,
											  .servername = "target_server",
											  .fdwname = "target_fdw" };
	AlterForeignServerStmt alter_server = { .type = T_AlterForeignServerStmt,
											.servername = "target_server",
											.version = "preflight",
											.has_version = true };
	RoleSpec mapping_user = { .type = T_RoleSpec, .roletype = ROLESPEC_CURRENT_USER };
	CreateUserMappingStmt create_mapping
		= { .type = T_CreateUserMappingStmt, .user = &mapping_user, .servername = "target_server" };
	String mapping_option_value = { .type = T_String, .sval = "enabled" };
	DefElem mapping_option = { .type = T_DefElem,
							   .defname = "g_preflight",
							   .arg = (Node *)&mapping_option_value,
							   .defaction = DEFELEM_ADD };
	ListCell mapping_cell = { .ptr_value = &mapping_option };
	List mapping_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &mapping_cell };
	AlterUserMappingStmt alter_mapping = { .type = T_AlterUserMappingStmt,
										   .user = &mapping_user,
										   .servername = "target_server",
										   .options = &mapping_options };
	DropUserMappingStmt drop_mapping
		= { .type = T_DropUserMappingStmt, .user = &mapping_user, .servername = "target_server" };
	String publish_value = { .type = T_String, .sval = "insert" };
	DefElem publish_option
		= { .type = T_DefElem, .defname = "publish", .arg = (Node *)&publish_value };
	ListCell publish_option_cell = { .ptr_value = &publish_option };
	List publish_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &publish_option_cell };
	PublicationTable publication_table
		= { .type = T_PublicationTable, .relation = &stats_relation };
	PublicationObjSpec publication_table_spec = { .type = T_PublicationObjSpec,
												  .pubobjtype = PUBLICATIONOBJ_TABLE,
												  .pubtable = &publication_table };
	PublicationObjSpec publication_schema_spec = { .type = T_PublicationObjSpec,
												   .pubobjtype = PUBLICATIONOBJ_TABLES_IN_SCHEMA,
												   .name = "target_schema" };
	ListCell publication_cells[2]
		= { { .ptr_value = &publication_table_spec }, { .ptr_value = &publication_schema_spec } };
	List publication_objects
		= { .type = T_List, .length = 2, .max_length = 2, .elements = publication_cells };
	CreatePublicationStmt create_publication = { .type = T_CreatePublicationStmt,
												 .pubname = "target_publication",
												 .options = &publish_options };
	CreatePublicationStmt create_publication_objects = { .type = T_CreatePublicationStmt,
														 .pubname = "target_publication",
														 .pubobjects = &publication_objects };
	AlterPublicationStmt alter_publication_options = { .type = T_AlterPublicationStmt,
													   .pubname = "target_publication",
													   .options = &publish_options };
	AlterPublicationStmt publication_add = { .type = T_AlterPublicationStmt,
											 .pubname = "target_publication",
											 .pubobjects = &publication_objects,
											 .action = AP_AddObjects };
	AlterPublicationStmt publication_drop = { .type = T_AlterPublicationStmt,
											  .pubname = "target_publication",
											  .pubobjects = &publication_objects,
											  .action = AP_DropObjects };
	AlterPublicationStmt publication_set = { .type = T_AlterPublicationStmt,
											 .pubname = "target_publication",
											 .pubobjects = &publication_objects,
											 .action = AP_SetObjects };
	String dependency_name = { .type = T_String, .sval = "target_object" };
	ListCell dependency_name_cell = { .ptr_value = &dependency_name };
	List dependency_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &dependency_name_cell };
	ObjectWithArgs dependency_routine = { .type = T_ObjectWithArgs, .objname = &dependency_names };
	String dependency_extension = { .type = T_String, .sval = "target_extension" };
	AlterObjectDependsStmt depends_function = { .type = T_AlterObjectDependsStmt,
												.objectType = OBJECT_FUNCTION,
												.object = (Node *)&dependency_routine,
												.extname = &dependency_extension };
	AlterObjectDependsStmt depends_procedure = { .type = T_AlterObjectDependsStmt,
												 .objectType = OBJECT_PROCEDURE,
												 .object = (Node *)&dependency_routine,
												 .extname = &dependency_extension };
	AlterObjectDependsStmt depends_routine = { .type = T_AlterObjectDependsStmt,
											   .objectType = OBJECT_ROUTINE,
											   .object = (Node *)&dependency_routine,
											   .extname = &dependency_extension };
	AlterObjectDependsStmt depends_trigger = { .type = T_AlterObjectDependsStmt,
											   .objectType = OBJECT_TRIGGER,
											   .relation = &stats_relation,
											   .object = (Node *)&dependency_names,
											   .extname = &dependency_extension };
	AlterObjectDependsStmt depends_index = { .type = T_AlterObjectDependsStmt,
											 .objectType = OBJECT_INDEX,
											 .relation = &stats_relation,
											 .extname = &dependency_extension };
	RoleSpec reassign_first
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "source_owner" };
	RoleSpec reassign_second
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "second_owner" };
	RoleSpec reassign_receiver
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	ListCell reassign_cells[2]
		= { { .ptr_value = &reassign_first }, { .ptr_value = &reassign_second } };
	List reassign_roles
		= { .type = T_List, .length = 2, .max_length = 2, .elements = reassign_cells };
	ReassignOwnedStmt reassign_named
		= { .type = T_ReassignOwnedStmt, .roles = &reassign_roles, .newrole = &reassign_receiver };
	RoleSpec reassign_self = { .type = T_RoleSpec, .roletype = ROLESPEC_CURRENT_USER };
	ListCell reassign_self_cell = { .ptr_value = &reassign_self };
	List reassign_self_roles
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &reassign_self_cell };
	ReassignOwnedStmt reassign_context
		= { .type = T_ReassignOwnedStmt, .roles = &reassign_self_roles, .newrole = &reassign_self };
	RangeVar foreign_relation = { .type = T_RangeVar,
								  .schemaname = "g_preflight",
								  .relname = "foreign_target",
								  .relpersistence = RELPERSISTENCE_PERMANENT };
	String foreign_option_value = { .type = T_String, .sval = "preflight" };
	DefElem foreign_option
		= { .type = T_DefElem, .defname = "g_preflight", .arg = (Node *)&foreign_option_value };
	ListCell foreign_option_cell = { .ptr_value = &foreign_option };
	List foreign_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &foreign_option_cell };
	CreateForeignTableStmt foreign_table = { .base = { .type = T_CreateForeignTableStmt,
													   .relation = &foreign_relation,
													   .oncommit = ONCOMMIT_NOOP },
											 .servername = "target_server",
											 .options = &foreign_options };
	RoleSpec owner_receiver
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	String owner_target = { .type = T_String, .sval = "owner_target" };
	String owner_schema_name = { .type = T_String, .sval = "pg_undo" };
	String owner_space_name = { .type = T_String, .sval = "g_preflight_tablespace" };
	AlterOwnerStmt owner_database = { .type = T_AlterOwnerStmt,
									  .objectType = OBJECT_DATABASE,
									  .object = (Node *)&owner_target,
									  .newowner = &owner_receiver };
	AlterOwnerStmt owner_schema = { .type = T_AlterOwnerStmt,
									.objectType = OBJECT_SCHEMA,
									.object = (Node *)&owner_schema_name,
									.newowner = &reassign_self };
	AlterOwnerStmt owner_function = { .type = T_AlterOwnerStmt,
									  .objectType = OBJECT_FUNCTION,
									  .object = (Node *)&dependency_routine,
									  .newowner = &owner_receiver };
	AlterOwnerStmt owner_collation = { .type = T_AlterOwnerStmt,
									   .objectType = OBJECT_COLLATION,
									   .object = (Node *)&dependency_names,
									   .newowner = &owner_receiver };
	AlterOwnerStmt owner_tablespace = { .type = T_AlterOwnerStmt,
										.objectType = OBJECT_TABLESPACE,
										.object = (Node *)&owner_space_name,
										.newowner = &owner_receiver };
	String database_template_value = { .type = T_String, .sval = "false" };
	String database_allow_value = { .type = T_String, .sval = "true" };
	Integer database_limit_value = { .type = T_Integer, .ival = 100 };
	DefElem database_template_option
		= { .type = T_DefElem, .defname = "is_template", .arg = (Node *)&database_template_value };
	DefElem database_allow_option = { .type = T_DefElem,
									  .defname = "allow_connections",
									  .arg = (Node *)&database_allow_value };
	DefElem database_limit_option = { .type = T_DefElem,
									  .defname = "connection_limit",
									  .arg = (Node *)&database_limit_value };
	ListCell database_flags_cells[2]
		= { { .ptr_value = &database_template_option }, { .ptr_value = &database_allow_option } };
	List database_flags_options
		= { .type = T_List, .length = 2, .max_length = 2, .elements = database_flags_cells };
	ListCell database_limit_cell = { .ptr_value = &database_limit_option };
	List database_limit_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &database_limit_cell };
	AlterDatabaseStmt database_flags = { .type = T_AlterDatabaseStmt,
										 .dbname = "target_database",
										 .options = &database_flags_options };
	AlterDatabaseStmt database_limit = { .type = T_AlterDatabaseStmt,
										 .dbname = "target_database",
										 .options = &database_limit_options };
	Integer tablespace_cost_value = { .type = T_Integer, .ival = 1 };
	DefElem tablespace_option
		= { .type = T_DefElem, .defname = "seq_page_cost", .arg = (Node *)&tablespace_cost_value };
	ListCell tablespace_option_cell = { .ptr_value = &tablespace_option };
	List tablespace_option_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &tablespace_option_cell };
	AlterTableSpaceOptionsStmt tablespace_options = { .type = T_AlterTableSpaceOptionsStmt,
													  .tablespacename = "g_preflight_tablespace",
													  .options = &tablespace_option_list };
	String label_name = { .type = T_String, .sval = "label_target" };
	String label_column_name = { .type = T_String, .sval = "label_column" };
	ListCell label_name_cell = { .ptr_value = &label_name };
	List label_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &label_name_cell };
	ListCell label_column_cells[2]
		= { { .ptr_value = &label_name }, { .ptr_value = &label_column_name } };
	List label_column_names
		= { .type = T_List, .length = 2, .max_length = 2, .elements = label_column_cells };
	ObjectWithArgs label_routine = { .type = T_ObjectWithArgs, .objname = &label_names };
	SecLabelStmt label_table = { .type = T_SecLabelStmt,
								 .objtype = OBJECT_TABLE,
								 .object = (Node *)&label_names,
								 .provider = "g_label_provider",
								 .label = "table_label" };
	SecLabelStmt label_column = { .type = T_SecLabelStmt,
								  .objtype = OBJECT_COLUMN,
								  .object = (Node *)&label_column_names };
	SecLabelStmt label_function = { .type = T_SecLabelStmt,
									.objtype = OBJECT_FUNCTION,
									.object = (Node *)&label_routine,
									.provider = "g_label_provider",
									.label = "routine_label" };
	SecLabelStmt label_database
		= { .type = T_SecLabelStmt, .objtype = OBJECT_DATABASE, .object = (Node *)&label_name };
	RangeVar static_relation = { .type = T_RangeVar,
								 .relname = "alter_target",
								 .inh = true,
								 .relpersistence = RELPERSISTENCE_PERMANENT };
	Integer static_stats_value = { .type = T_Integer, .ival = 100 };
	AlterTableCmd static_stats_cmd = { .type = T_AlterTableCmd,
									   .subtype = AT_SetStatistics,
									   .name = "target_column",
									   .def = (Node *)&static_stats_value };
	RoleSpec static_owner_role
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	AlterTableCmd static_owner_cmd
		= { .type = T_AlterTableCmd, .subtype = AT_ChangeOwner, .newowner = &static_owner_role };
	Integer static_fillfactor = { .type = T_Integer, .ival = 90 };
	DefElem static_set_option
		= { .type = T_DefElem, .defname = "fillfactor", .arg = (Node *)&static_fillfactor };
	DefElem static_reset_option = { .type = T_DefElem, .defname = "autovacuum_enabled" };
	ListCell static_set_cell = { .ptr_value = &static_set_option };
	ListCell static_reset_cell = { .ptr_value = &static_reset_option };
	List static_set_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_set_cell };
	List static_reset_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_reset_cell };
	AlterTableCmd static_set_cmd
		= { .type = T_AlterTableCmd, .subtype = AT_SetRelOptions, .def = (Node *)&static_set_list };
	AlterTableCmd static_reset_cmd = { .type = T_AlterTableCmd,
									   .subtype = AT_ResetRelOptions,
									   .def = (Node *)&static_reset_list };
	ListCell static_stats_cell = { .ptr_value = &static_stats_cmd };
	ListCell static_owner_cell = { .ptr_value = &static_owner_cmd };
	ListCell static_option_cells[2]
		= { { .ptr_value = &static_set_cmd }, { .ptr_value = &static_reset_cmd } };
	List static_stats_cmds
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_stats_cell };
	List static_owner_cmds
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_owner_cell };
	List static_option_cmds
		= { .type = T_List, .length = 2, .max_length = 2, .elements = static_option_cells };
	AlterTableStmt static_statistics = { .type = T_AlterTableStmt,
										 .objtype = OBJECT_TABLE,
										 .relation = &static_relation,
										 .cmds = &static_stats_cmds };
	AlterTableStmt static_owner = { .type = T_AlterTableStmt,
									.objtype = OBJECT_TABLE,
									.relation = &static_relation,
									.cmds = &static_owner_cmds };
	AlterTableStmt static_options = { .type = T_AlterTableStmt,
									  .objtype = OBJECT_TABLE,
									  .relation = &static_relation,
									  .cmds = &static_option_cmds };
	RangeVar import_first = { .type = T_RangeVar, .relname = "import_first", .location = -1 };
	RangeVar import_second = { .type = T_RangeVar, .relname = "import_second", .location = -1 };
	ListCell import_table_cells[2]
		= { { .ptr_value = &import_first }, { .ptr_value = &import_second } };
	List import_tables
		= { .type = T_List, .length = 2, .max_length = 2, .elements = import_table_cells };
	String import_option_value = { .type = T_String, .sval = "true" };
	DefElem import_option
		= { .type = T_DefElem, .defname = "import_default", .arg = (Node *)&import_option_value };
	ListCell import_option_cell = { .ptr_value = &import_option };
	List import_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &import_option_cell };
	ImportForeignSchemaStmt import_all = { .type = T_ImportForeignSchemaStmt,
										   .server_name = "target_server",
										   .remote_schema = "remote_schema",
										   .local_schema = "local_schema",
										   .list_type = FDW_IMPORT_SCHEMA_ALL };
	ImportForeignSchemaStmt import_limit = { .type = T_ImportForeignSchemaStmt,
											 .server_name = "target_server",
											 .remote_schema = "remote_schema",
											 .local_schema = "local_schema",
											 .list_type = FDW_IMPORT_SCHEMA_LIMIT_TO,
											 .table_list = &import_tables,
											 .options = &import_options };
	ImportForeignSchemaStmt import_except = { .type = T_ImportForeignSchemaStmt,
											  .server_name = "target_server",
											  .remote_schema = "remote_schema",
											  .local_schema = "local_schema",
											  .list_type = FDW_IMPORT_SCHEMA_EXCEPT,
											  .table_list = &import_tables,
											  .options = &import_options };
	AlterExtensionContentsStmt extension_add_table = { .type = T_AlterExtensionContentsStmt,
													   .extname = "existing_extension",
													   .action = 1,
													   .objtype = OBJECT_TABLE,
													   .object = (Node *)&dependency_names };
	AlterExtensionContentsStmt extension_drop_table = { .type = T_AlterExtensionContentsStmt,
														.extname = "existing_extension",
														.action = -1,
														.objtype = OBJECT_TABLE,
														.object = (Node *)&dependency_names };
	AlterExtensionContentsStmt extension_add_function = { .type = T_AlterExtensionContentsStmt,
														  .extname = "existing_extension",
														  .action = 1,
														  .objtype = OBJECT_FUNCTION,
														  .object = (Node *)&dependency_routine };
	AlterExtensionContentsStmt extension_drop_function = { .type = T_AlterExtensionContentsStmt,
														   .extname = "existing_extension",
														   .action = -1,
														   .objtype = OBJECT_FUNCTION,
														   .object = (Node *)&dependency_routine };
	String extension_version = { .type = T_String, .sval = "2.0" };
	DefElem version_option
		= { .type = T_DefElem, .defname = "new_version", .arg = (Node *)&extension_version };
	ListCell version_cell = { .ptr_value = &version_option };
	List version_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &version_cell };
	AlterExtensionStmt extension_default
		= { .type = T_AlterExtensionStmt, .extname = "existing_extension" };
	AlterExtensionStmt extension_explicit = { .type = T_AlterExtensionStmt,
											  .extname = "existing_extension",
											  .options = &version_options };
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
						   (Node *)&access_method,
						   (Node *)&create_fdw,
						   (Node *)&alter_fdw,
						   (Node *)&create_server,
						   (Node *)&alter_server,
						   (Node *)&create_mapping,
						   (Node *)&alter_mapping,
						   (Node *)&drop_mapping,
						   (Node *)&create_publication,
						   (Node *)&create_publication_objects,
						   (Node *)&alter_publication_options,
						   (Node *)&publication_add,
						   (Node *)&publication_drop,
						   (Node *)&publication_set,
						   (Node *)&depends_function,
						   (Node *)&depends_procedure,
						   (Node *)&depends_routine,
						   (Node *)&depends_trigger,
						   (Node *)&depends_index,
						   (Node *)&reassign_named,
						   (Node *)&reassign_context,
						   (Node *)&foreign_table,
						   (Node *)&owner_database,
						   (Node *)&owner_schema,
						   (Node *)&owner_function,
						   (Node *)&owner_collation,
						   (Node *)&owner_tablespace,
						   (Node *)&database_flags,
						   (Node *)&database_limit,
						   (Node *)&tablespace_options,
						   (Node *)&label_table,
						   (Node *)&label_column,
						   (Node *)&label_function,
						   (Node *)&label_database,
						   (Node *)&static_statistics,
						   (Node *)&static_owner,
						   (Node *)&static_options,
						   (Node *)&import_all,
						   (Node *)&import_limit,
						   (Node *)&import_except,
						   (Node *)&extension_add_table,
						   (Node *)&extension_drop_table,
						   (Node *)&extension_add_function,
						   (Node *)&extension_drop_function,
						   (Node *)&extension_default,
						   (Node *)&extension_explicit };

	for (unsigned i = 0; i < lengthof(statements); i++) {
		int caught;

		reset_create_preflight();
		utility_bind_ok = false;
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			create_sql_entry(statements[i]);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(error_code, ERRCODE_DATA_CORRUPTED);
		if (IsA(statements[i], SecLabelStmt) || IsA(statements[i], AlterTableStmt)
			|| IsA(statements[i], AlterExtensionContentsStmt)
			|| IsA(statements[i], AlterExtensionStmt))
			UT_ASSERT(strcmp(error_message,
							 "canonical ACTIVE publication failed before shared catalog alteration")
					  == 0);
		if (IsA(statements[i], ImportForeignSchemaStmt))
			UT_ASSERT(strcmp(error_message,
							 "canonical ACTIVE publication failed before shared catalog creation")
					  == 0);
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
	DefElem no_validator = { .type = T_DefElem, .defname = "validator" };
	ListCell validator_cell = { .ptr_value = &no_validator };
	List validator_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &validator_cell };
	CreateFdwStmt create_fdw
		= { .type = T_CreateFdwStmt, .fdwname = "target_fdw", .func_options = &validator_options };
	AlterFdwStmt alter_fdw
		= { .type = T_AlterFdwStmt, .fdwname = "target_fdw", .func_options = &validator_options };
	CreateForeignServerStmt create_server = { .type = T_CreateForeignServerStmt,
											  .servername = "target_server",
											  .fdwname = "target_fdw" };
	AlterForeignServerStmt alter_server = { .type = T_AlterForeignServerStmt,
											.servername = "target_server",
											.version = "preflight",
											.has_version = true };
	RoleSpec mapping_user = { .type = T_RoleSpec, .roletype = ROLESPEC_CURRENT_USER };
	CreateUserMappingStmt create_mapping
		= { .type = T_CreateUserMappingStmt, .user = &mapping_user, .servername = "target_server" };
	String mapping_option_value = { .type = T_String, .sval = "enabled" };
	DefElem mapping_option = { .type = T_DefElem,
							   .defname = "g_preflight",
							   .arg = (Node *)&mapping_option_value,
							   .defaction = DEFELEM_ADD };
	ListCell mapping_cell = { .ptr_value = &mapping_option };
	List mapping_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &mapping_cell };
	AlterUserMappingStmt alter_mapping = { .type = T_AlterUserMappingStmt,
										   .user = &mapping_user,
										   .servername = "target_server",
										   .options = &mapping_options };
	DropUserMappingStmt drop_mapping
		= { .type = T_DropUserMappingStmt, .user = &mapping_user, .servername = "target_server" };
	String publish_value = { .type = T_String, .sval = "insert" };
	DefElem publish_option
		= { .type = T_DefElem, .defname = "publish", .arg = (Node *)&publish_value };
	ListCell publish_option_cell = { .ptr_value = &publish_option };
	List publish_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &publish_option_cell };
	PublicationTable publication_table
		= { .type = T_PublicationTable, .relation = &stats_relation };
	PublicationObjSpec publication_table_spec = { .type = T_PublicationObjSpec,
												  .pubobjtype = PUBLICATIONOBJ_TABLE,
												  .pubtable = &publication_table };
	PublicationObjSpec publication_schema_spec = { .type = T_PublicationObjSpec,
												   .pubobjtype = PUBLICATIONOBJ_TABLES_IN_SCHEMA,
												   .name = "target_schema" };
	ListCell publication_cells[2]
		= { { .ptr_value = &publication_table_spec }, { .ptr_value = &publication_schema_spec } };
	List publication_objects
		= { .type = T_List, .length = 2, .max_length = 2, .elements = publication_cells };
	CreatePublicationStmt create_publication = { .type = T_CreatePublicationStmt,
												 .pubname = "target_publication",
												 .options = &publish_options };
	CreatePublicationStmt create_publication_objects = { .type = T_CreatePublicationStmt,
														 .pubname = "target_publication",
														 .pubobjects = &publication_objects };
	AlterPublicationStmt alter_publication_options = { .type = T_AlterPublicationStmt,
													   .pubname = "target_publication",
													   .options = &publish_options };
	AlterPublicationStmt publication_add = { .type = T_AlterPublicationStmt,
											 .pubname = "target_publication",
											 .pubobjects = &publication_objects,
											 .action = AP_AddObjects };
	AlterPublicationStmt publication_drop = { .type = T_AlterPublicationStmt,
											  .pubname = "target_publication",
											  .pubobjects = &publication_objects,
											  .action = AP_DropObjects };
	AlterPublicationStmt publication_set = { .type = T_AlterPublicationStmt,
											 .pubname = "target_publication",
											 .pubobjects = &publication_objects,
											 .action = AP_SetObjects };
	String dependency_name = { .type = T_String, .sval = "target_object" };
	ListCell dependency_name_cell = { .ptr_value = &dependency_name };
	List dependency_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &dependency_name_cell };
	ObjectWithArgs dependency_routine = { .type = T_ObjectWithArgs, .objname = &dependency_names };
	String dependency_extension = { .type = T_String, .sval = "target_extension" };
	AlterObjectDependsStmt depends_function = { .type = T_AlterObjectDependsStmt,
												.objectType = OBJECT_FUNCTION,
												.object = (Node *)&dependency_routine,
												.extname = &dependency_extension };
	AlterObjectDependsStmt depends_procedure = { .type = T_AlterObjectDependsStmt,
												 .objectType = OBJECT_PROCEDURE,
												 .object = (Node *)&dependency_routine,
												 .extname = &dependency_extension };
	AlterObjectDependsStmt depends_routine = { .type = T_AlterObjectDependsStmt,
											   .objectType = OBJECT_ROUTINE,
											   .object = (Node *)&dependency_routine,
											   .extname = &dependency_extension };
	AlterObjectDependsStmt depends_trigger = { .type = T_AlterObjectDependsStmt,
											   .objectType = OBJECT_TRIGGER,
											   .relation = &stats_relation,
											   .object = (Node *)&dependency_names,
											   .extname = &dependency_extension };
	AlterObjectDependsStmt depends_index = { .type = T_AlterObjectDependsStmt,
											 .objectType = OBJECT_INDEX,
											 .relation = &stats_relation,
											 .extname = &dependency_extension };
	RoleSpec reassign_first
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "source_owner" };
	RoleSpec reassign_second
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "second_owner" };
	RoleSpec reassign_receiver
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	ListCell reassign_cells[2]
		= { { .ptr_value = &reassign_first }, { .ptr_value = &reassign_second } };
	List reassign_roles
		= { .type = T_List, .length = 2, .max_length = 2, .elements = reassign_cells };
	ReassignOwnedStmt reassign_named
		= { .type = T_ReassignOwnedStmt, .roles = &reassign_roles, .newrole = &reassign_receiver };
	RoleSpec reassign_self = { .type = T_RoleSpec, .roletype = ROLESPEC_CURRENT_USER };
	ListCell reassign_self_cell = { .ptr_value = &reassign_self };
	List reassign_self_roles
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &reassign_self_cell };
	ReassignOwnedStmt reassign_context
		= { .type = T_ReassignOwnedStmt, .roles = &reassign_self_roles, .newrole = &reassign_self };
	RangeVar foreign_relation = { .type = T_RangeVar,
								  .schemaname = "g_preflight",
								  .relname = "foreign_target",
								  .relpersistence = RELPERSISTENCE_PERMANENT };
	String foreign_option_value = { .type = T_String, .sval = "preflight" };
	DefElem foreign_option
		= { .type = T_DefElem, .defname = "g_preflight", .arg = (Node *)&foreign_option_value };
	ListCell foreign_option_cell = { .ptr_value = &foreign_option };
	List foreign_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &foreign_option_cell };
	CreateForeignTableStmt foreign_table = { .base = { .type = T_CreateForeignTableStmt,
													   .relation = &foreign_relation,
													   .oncommit = ONCOMMIT_NOOP },
											 .servername = "target_server",
											 .options = &foreign_options };
	RoleSpec owner_receiver
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	String owner_target = { .type = T_String, .sval = "owner_target" };
	String owner_schema_name = { .type = T_String, .sval = "pg_undo" };
	String owner_space_name = { .type = T_String, .sval = "g_preflight_tablespace" };
	AlterOwnerStmt owner_database = { .type = T_AlterOwnerStmt,
									  .objectType = OBJECT_DATABASE,
									  .object = (Node *)&owner_target,
									  .newowner = &owner_receiver };
	AlterOwnerStmt owner_schema = { .type = T_AlterOwnerStmt,
									.objectType = OBJECT_SCHEMA,
									.object = (Node *)&owner_schema_name,
									.newowner = &reassign_self };
	AlterOwnerStmt owner_function = { .type = T_AlterOwnerStmt,
									  .objectType = OBJECT_FUNCTION,
									  .object = (Node *)&dependency_routine,
									  .newowner = &owner_receiver };
	AlterOwnerStmt owner_collation = { .type = T_AlterOwnerStmt,
									   .objectType = OBJECT_COLLATION,
									   .object = (Node *)&dependency_names,
									   .newowner = &owner_receiver };
	AlterOwnerStmt owner_tablespace = { .type = T_AlterOwnerStmt,
										.objectType = OBJECT_TABLESPACE,
										.object = (Node *)&owner_space_name,
										.newowner = &owner_receiver };
	String database_template_value = { .type = T_String, .sval = "false" };
	String database_allow_value = { .type = T_String, .sval = "true" };
	Integer database_limit_value = { .type = T_Integer, .ival = 100 };
	DefElem database_template_option
		= { .type = T_DefElem, .defname = "is_template", .arg = (Node *)&database_template_value };
	DefElem database_allow_option = { .type = T_DefElem,
									  .defname = "allow_connections",
									  .arg = (Node *)&database_allow_value };
	DefElem database_limit_option = { .type = T_DefElem,
									  .defname = "connection_limit",
									  .arg = (Node *)&database_limit_value };
	ListCell database_flags_cells[2]
		= { { .ptr_value = &database_template_option }, { .ptr_value = &database_allow_option } };
	List database_flags_options
		= { .type = T_List, .length = 2, .max_length = 2, .elements = database_flags_cells };
	ListCell database_limit_cell = { .ptr_value = &database_limit_option };
	List database_limit_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &database_limit_cell };
	AlterDatabaseStmt database_flags = { .type = T_AlterDatabaseStmt,
										 .dbname = "target_database",
										 .options = &database_flags_options };
	AlterDatabaseStmt database_limit = { .type = T_AlterDatabaseStmt,
										 .dbname = "target_database",
										 .options = &database_limit_options };
	Integer tablespace_cost_value = { .type = T_Integer, .ival = 1 };
	DefElem tablespace_option
		= { .type = T_DefElem, .defname = "seq_page_cost", .arg = (Node *)&tablespace_cost_value };
	ListCell tablespace_option_cell = { .ptr_value = &tablespace_option };
	List tablespace_option_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &tablespace_option_cell };
	AlterTableSpaceOptionsStmt tablespace_options = { .type = T_AlterTableSpaceOptionsStmt,
													  .tablespacename = "g_preflight_tablespace",
													  .options = &tablespace_option_list };
	String label_name = { .type = T_String, .sval = "label_target" };
	String label_column_name = { .type = T_String, .sval = "label_column" };
	ListCell label_name_cell = { .ptr_value = &label_name };
	List label_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &label_name_cell };
	ListCell label_column_cells[2]
		= { { .ptr_value = &label_name }, { .ptr_value = &label_column_name } };
	List label_column_names
		= { .type = T_List, .length = 2, .max_length = 2, .elements = label_column_cells };
	ObjectWithArgs label_routine = { .type = T_ObjectWithArgs, .objname = &label_names };
	SecLabelStmt label_table = { .type = T_SecLabelStmt,
								 .objtype = OBJECT_TABLE,
								 .object = (Node *)&label_names,
								 .provider = "g_label_provider",
								 .label = "table_label" };
	SecLabelStmt label_column = { .type = T_SecLabelStmt,
								  .objtype = OBJECT_COLUMN,
								  .object = (Node *)&label_column_names };
	SecLabelStmt label_function = { .type = T_SecLabelStmt,
									.objtype = OBJECT_FUNCTION,
									.object = (Node *)&label_routine,
									.provider = "g_label_provider",
									.label = "routine_label" };
	SecLabelStmt label_database
		= { .type = T_SecLabelStmt, .objtype = OBJECT_DATABASE, .object = (Node *)&label_name };
	RangeVar static_relation = { .type = T_RangeVar,
								 .relname = "alter_target",
								 .inh = true,
								 .relpersistence = RELPERSISTENCE_PERMANENT };
	Integer static_stats_value = { .type = T_Integer, .ival = 100 };
	AlterTableCmd static_stats_cmd = { .type = T_AlterTableCmd,
									   .subtype = AT_SetStatistics,
									   .name = "target_column",
									   .def = (Node *)&static_stats_value };
	RoleSpec static_owner_role
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	AlterTableCmd static_owner_cmd
		= { .type = T_AlterTableCmd, .subtype = AT_ChangeOwner, .newowner = &static_owner_role };
	Integer static_fillfactor = { .type = T_Integer, .ival = 90 };
	DefElem static_set_option
		= { .type = T_DefElem, .defname = "fillfactor", .arg = (Node *)&static_fillfactor };
	DefElem static_reset_option = { .type = T_DefElem, .defname = "autovacuum_enabled" };
	ListCell static_set_cell = { .ptr_value = &static_set_option };
	ListCell static_reset_cell = { .ptr_value = &static_reset_option };
	List static_set_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_set_cell };
	List static_reset_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_reset_cell };
	AlterTableCmd static_set_cmd
		= { .type = T_AlterTableCmd, .subtype = AT_SetRelOptions, .def = (Node *)&static_set_list };
	AlterTableCmd static_reset_cmd = { .type = T_AlterTableCmd,
									   .subtype = AT_ResetRelOptions,
									   .def = (Node *)&static_reset_list };
	ListCell static_stats_cell = { .ptr_value = &static_stats_cmd };
	ListCell static_owner_cell = { .ptr_value = &static_owner_cmd };
	ListCell static_option_cells[2]
		= { { .ptr_value = &static_set_cmd }, { .ptr_value = &static_reset_cmd } };
	List static_stats_cmds
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_stats_cell };
	List static_owner_cmds
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_owner_cell };
	List static_option_cmds
		= { .type = T_List, .length = 2, .max_length = 2, .elements = static_option_cells };
	AlterTableStmt static_statistics = { .type = T_AlterTableStmt,
										 .objtype = OBJECT_TABLE,
										 .relation = &static_relation,
										 .cmds = &static_stats_cmds };
	AlterTableStmt static_owner = { .type = T_AlterTableStmt,
									.objtype = OBJECT_TABLE,
									.relation = &static_relation,
									.cmds = &static_owner_cmds };
	AlterTableStmt static_options = { .type = T_AlterTableStmt,
									  .objtype = OBJECT_TABLE,
									  .relation = &static_relation,
									  .cmds = &static_option_cmds };
	RangeVar import_first = { .type = T_RangeVar, .relname = "import_first", .location = -1 };
	RangeVar import_second = { .type = T_RangeVar, .relname = "import_second", .location = -1 };
	ListCell import_table_cells[2]
		= { { .ptr_value = &import_first }, { .ptr_value = &import_second } };
	List import_tables
		= { .type = T_List, .length = 2, .max_length = 2, .elements = import_table_cells };
	String import_option_value = { .type = T_String, .sval = "true" };
	DefElem import_option
		= { .type = T_DefElem, .defname = "import_default", .arg = (Node *)&import_option_value };
	ListCell import_option_cell = { .ptr_value = &import_option };
	List import_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &import_option_cell };
	ImportForeignSchemaStmt import_all = { .type = T_ImportForeignSchemaStmt,
										   .server_name = "target_server",
										   .remote_schema = "remote_schema",
										   .local_schema = "local_schema",
										   .list_type = FDW_IMPORT_SCHEMA_ALL };
	ImportForeignSchemaStmt import_limit = { .type = T_ImportForeignSchemaStmt,
											 .server_name = "target_server",
											 .remote_schema = "remote_schema",
											 .local_schema = "local_schema",
											 .list_type = FDW_IMPORT_SCHEMA_LIMIT_TO,
											 .table_list = &import_tables,
											 .options = &import_options };
	ImportForeignSchemaStmt import_except = { .type = T_ImportForeignSchemaStmt,
											  .server_name = "target_server",
											  .remote_schema = "remote_schema",
											  .local_schema = "local_schema",
											  .list_type = FDW_IMPORT_SCHEMA_EXCEPT,
											  .table_list = &import_tables,
											  .options = &import_options };
	AlterExtensionContentsStmt extension_add_table = { .type = T_AlterExtensionContentsStmt,
													   .extname = "existing_extension",
													   .action = 1,
													   .objtype = OBJECT_TABLE,
													   .object = (Node *)&dependency_names };
	AlterExtensionContentsStmt extension_drop_table = { .type = T_AlterExtensionContentsStmt,
														.extname = "existing_extension",
														.action = -1,
														.objtype = OBJECT_TABLE,
														.object = (Node *)&dependency_names };
	AlterExtensionContentsStmt extension_add_function = { .type = T_AlterExtensionContentsStmt,
														  .extname = "existing_extension",
														  .action = 1,
														  .objtype = OBJECT_FUNCTION,
														  .object = (Node *)&dependency_routine };
	AlterExtensionContentsStmt extension_drop_function = { .type = T_AlterExtensionContentsStmt,
														   .extname = "existing_extension",
														   .action = -1,
														   .objtype = OBJECT_FUNCTION,
														   .object = (Node *)&dependency_routine };
	String extension_version = { .type = T_String, .sval = "2.0" };
	DefElem version_option
		= { .type = T_DefElem, .defname = "new_version", .arg = (Node *)&extension_version };
	ListCell version_cell = { .ptr_value = &version_option };
	List version_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &version_cell };
	AlterExtensionStmt extension_default
		= { .type = T_AlterExtensionStmt, .extname = "existing_extension" };
	AlterExtensionStmt extension_explicit = { .type = T_AlterExtensionStmt,
											  .extname = "existing_extension",
											  .options = &version_options };
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
						   (Node *)&access_method,
						   (Node *)&create_fdw,
						   (Node *)&alter_fdw,
						   (Node *)&create_server,
						   (Node *)&alter_server,
						   (Node *)&create_mapping,
						   (Node *)&alter_mapping,
						   (Node *)&drop_mapping,
						   (Node *)&create_publication,
						   (Node *)&create_publication_objects,
						   (Node *)&alter_publication_options,
						   (Node *)&publication_add,
						   (Node *)&publication_drop,
						   (Node *)&publication_set,
						   (Node *)&depends_function,
						   (Node *)&depends_procedure,
						   (Node *)&depends_routine,
						   (Node *)&depends_trigger,
						   (Node *)&depends_index,
						   (Node *)&reassign_named,
						   (Node *)&reassign_context,
						   (Node *)&foreign_table,
						   (Node *)&owner_database,
						   (Node *)&owner_schema,
						   (Node *)&owner_function,
						   (Node *)&owner_collation,
						   (Node *)&owner_tablespace,
						   (Node *)&database_flags,
						   (Node *)&database_limit,
						   (Node *)&tablespace_options,
						   (Node *)&label_table,
						   (Node *)&label_column,
						   (Node *)&label_function,
						   (Node *)&label_database,
						   (Node *)&static_statistics,
						   (Node *)&static_owner,
						   (Node *)&static_options,
						   (Node *)&import_all,
						   (Node *)&import_limit,
						   (Node *)&import_except,
						   (Node *)&extension_add_table,
						   (Node *)&extension_drop_table,
						   (Node *)&extension_add_function,
						   (Node *)&extension_drop_function,
						   (Node *)&extension_default,
						   (Node *)&extension_explicit };

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
			if (child
				&& (IsA(statements[i], SecLabelStmt) || IsA(statements[i], AlterTableStmt)
					|| IsA(statements[i], AlterExtensionContentsStmt)
					|| IsA(statements[i], AlterExtensionStmt)))
				UT_ASSERT(strcmp(error_message,
								 "shared catalog alteration does not support subtransactions")
						  == 0);
			if (child && IsA(statements[i], ImportForeignSchemaStmt))
				UT_ASSERT(strcmp(error_message,
								 "shared catalog creation does not support subtransactions")
						  == 0);
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
	DefElem no_validator = { .type = T_DefElem, .defname = "validator" };
	ListCell validator_cell = { .ptr_value = &no_validator };
	List validator_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &validator_cell };
	CreateFdwStmt create_fdw
		= { .type = T_CreateFdwStmt, .fdwname = "target_fdw", .func_options = &validator_options };
	AlterFdwStmt alter_fdw
		= { .type = T_AlterFdwStmt, .fdwname = "target_fdw", .func_options = &validator_options };
	CreateForeignServerStmt create_server = { .type = T_CreateForeignServerStmt,
											  .servername = "target_server",
											  .fdwname = "target_fdw" };
	AlterForeignServerStmt alter_server = { .type = T_AlterForeignServerStmt,
											.servername = "target_server",
											.version = "preflight",
											.has_version = true };
	RoleSpec mapping_user = { .type = T_RoleSpec, .roletype = ROLESPEC_CURRENT_USER };
	CreateUserMappingStmt create_mapping
		= { .type = T_CreateUserMappingStmt, .user = &mapping_user, .servername = "target_server" };
	String mapping_option_value = { .type = T_String, .sval = "enabled" };
	DefElem mapping_option = { .type = T_DefElem,
							   .defname = "g_preflight",
							   .arg = (Node *)&mapping_option_value,
							   .defaction = DEFELEM_ADD };
	ListCell mapping_cell = { .ptr_value = &mapping_option };
	List mapping_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &mapping_cell };
	AlterUserMappingStmt alter_mapping = { .type = T_AlterUserMappingStmt,
										   .user = &mapping_user,
										   .servername = "target_server",
										   .options = &mapping_options };
	DropUserMappingStmt drop_mapping
		= { .type = T_DropUserMappingStmt, .user = &mapping_user, .servername = "target_server" };
	String publish_value = { .type = T_String, .sval = "insert" };
	DefElem publish_option
		= { .type = T_DefElem, .defname = "publish", .arg = (Node *)&publish_value };
	ListCell publish_option_cell = { .ptr_value = &publish_option };
	List publish_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &publish_option_cell };
	PublicationTable publication_table
		= { .type = T_PublicationTable, .relation = &stats_relation };
	PublicationObjSpec publication_table_spec = { .type = T_PublicationObjSpec,
												  .pubobjtype = PUBLICATIONOBJ_TABLE,
												  .pubtable = &publication_table };
	PublicationObjSpec publication_schema_spec = { .type = T_PublicationObjSpec,
												   .pubobjtype = PUBLICATIONOBJ_TABLES_IN_SCHEMA,
												   .name = "target_schema" };
	ListCell publication_cells[2]
		= { { .ptr_value = &publication_table_spec }, { .ptr_value = &publication_schema_spec } };
	List publication_objects
		= { .type = T_List, .length = 2, .max_length = 2, .elements = publication_cells };
	CreatePublicationStmt create_publication = { .type = T_CreatePublicationStmt,
												 .pubname = "target_publication",
												 .options = &publish_options };
	CreatePublicationStmt create_publication_objects = { .type = T_CreatePublicationStmt,
														 .pubname = "target_publication",
														 .pubobjects = &publication_objects };
	AlterPublicationStmt alter_publication_options = { .type = T_AlterPublicationStmt,
													   .pubname = "target_publication",
													   .options = &publish_options };
	AlterPublicationStmt publication_add = { .type = T_AlterPublicationStmt,
											 .pubname = "target_publication",
											 .pubobjects = &publication_objects,
											 .action = AP_AddObjects };
	AlterPublicationStmt publication_drop = { .type = T_AlterPublicationStmt,
											  .pubname = "target_publication",
											  .pubobjects = &publication_objects,
											  .action = AP_DropObjects };
	AlterPublicationStmt publication_set = { .type = T_AlterPublicationStmt,
											 .pubname = "target_publication",
											 .pubobjects = &publication_objects,
											 .action = AP_SetObjects };
	String dependency_name = { .type = T_String, .sval = "target_object" };
	ListCell dependency_name_cell = { .ptr_value = &dependency_name };
	List dependency_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &dependency_name_cell };
	ObjectWithArgs dependency_routine = { .type = T_ObjectWithArgs, .objname = &dependency_names };
	String dependency_extension = { .type = T_String, .sval = "target_extension" };
	AlterObjectDependsStmt depends_function = { .type = T_AlterObjectDependsStmt,
												.objectType = OBJECT_FUNCTION,
												.object = (Node *)&dependency_routine,
												.extname = &dependency_extension };
	AlterObjectDependsStmt depends_procedure = { .type = T_AlterObjectDependsStmt,
												 .objectType = OBJECT_PROCEDURE,
												 .object = (Node *)&dependency_routine,
												 .extname = &dependency_extension };
	AlterObjectDependsStmt depends_routine = { .type = T_AlterObjectDependsStmt,
											   .objectType = OBJECT_ROUTINE,
											   .object = (Node *)&dependency_routine,
											   .extname = &dependency_extension };
	AlterObjectDependsStmt depends_trigger = { .type = T_AlterObjectDependsStmt,
											   .objectType = OBJECT_TRIGGER,
											   .relation = &stats_relation,
											   .object = (Node *)&dependency_names,
											   .extname = &dependency_extension };
	AlterObjectDependsStmt depends_index = { .type = T_AlterObjectDependsStmt,
											 .objectType = OBJECT_INDEX,
											 .relation = &stats_relation,
											 .extname = &dependency_extension };
	RoleSpec reassign_first
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "source_owner" };
	RoleSpec reassign_second
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "second_owner" };
	RoleSpec reassign_receiver
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	ListCell reassign_cells[2]
		= { { .ptr_value = &reassign_first }, { .ptr_value = &reassign_second } };
	List reassign_roles
		= { .type = T_List, .length = 2, .max_length = 2, .elements = reassign_cells };
	ReassignOwnedStmt reassign_named
		= { .type = T_ReassignOwnedStmt, .roles = &reassign_roles, .newrole = &reassign_receiver };
	RoleSpec reassign_self = { .type = T_RoleSpec, .roletype = ROLESPEC_CURRENT_USER };
	ListCell reassign_self_cell = { .ptr_value = &reassign_self };
	List reassign_self_roles
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &reassign_self_cell };
	ReassignOwnedStmt reassign_context
		= { .type = T_ReassignOwnedStmt, .roles = &reassign_self_roles, .newrole = &reassign_self };
	RangeVar foreign_relation = { .type = T_RangeVar,
								  .schemaname = "g_preflight",
								  .relname = "foreign_target",
								  .relpersistence = RELPERSISTENCE_PERMANENT };
	String foreign_option_value = { .type = T_String, .sval = "preflight" };
	DefElem foreign_option
		= { .type = T_DefElem, .defname = "g_preflight", .arg = (Node *)&foreign_option_value };
	ListCell foreign_option_cell = { .ptr_value = &foreign_option };
	List foreign_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &foreign_option_cell };
	CreateForeignTableStmt foreign_table = { .base = { .type = T_CreateForeignTableStmt,
													   .relation = &foreign_relation,
													   .oncommit = ONCOMMIT_NOOP },
											 .servername = "target_server",
											 .options = &foreign_options };
	RoleSpec owner_receiver
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	String owner_target = { .type = T_String, .sval = "owner_target" };
	String owner_schema_name = { .type = T_String, .sval = "pg_undo" };
	String owner_space_name = { .type = T_String, .sval = "g_preflight_tablespace" };
	AlterOwnerStmt owner_database = { .type = T_AlterOwnerStmt,
									  .objectType = OBJECT_DATABASE,
									  .object = (Node *)&owner_target,
									  .newowner = &owner_receiver };
	AlterOwnerStmt owner_schema = { .type = T_AlterOwnerStmt,
									.objectType = OBJECT_SCHEMA,
									.object = (Node *)&owner_schema_name,
									.newowner = &reassign_self };
	AlterOwnerStmt owner_function = { .type = T_AlterOwnerStmt,
									  .objectType = OBJECT_FUNCTION,
									  .object = (Node *)&dependency_routine,
									  .newowner = &owner_receiver };
	AlterOwnerStmt owner_collation = { .type = T_AlterOwnerStmt,
									   .objectType = OBJECT_COLLATION,
									   .object = (Node *)&dependency_names,
									   .newowner = &owner_receiver };
	AlterOwnerStmt owner_tablespace = { .type = T_AlterOwnerStmt,
										.objectType = OBJECT_TABLESPACE,
										.object = (Node *)&owner_space_name,
										.newowner = &owner_receiver };
	String database_template_value = { .type = T_String, .sval = "false" };
	String database_allow_value = { .type = T_String, .sval = "true" };
	Integer database_limit_value = { .type = T_Integer, .ival = 100 };
	DefElem database_template_option
		= { .type = T_DefElem, .defname = "is_template", .arg = (Node *)&database_template_value };
	DefElem database_allow_option = { .type = T_DefElem,
									  .defname = "allow_connections",
									  .arg = (Node *)&database_allow_value };
	DefElem database_limit_option = { .type = T_DefElem,
									  .defname = "connection_limit",
									  .arg = (Node *)&database_limit_value };
	ListCell database_flags_cells[2]
		= { { .ptr_value = &database_template_option }, { .ptr_value = &database_allow_option } };
	List database_flags_options
		= { .type = T_List, .length = 2, .max_length = 2, .elements = database_flags_cells };
	ListCell database_limit_cell = { .ptr_value = &database_limit_option };
	List database_limit_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &database_limit_cell };
	AlterDatabaseStmt database_flags = { .type = T_AlterDatabaseStmt,
										 .dbname = "target_database",
										 .options = &database_flags_options };
	AlterDatabaseStmt database_limit = { .type = T_AlterDatabaseStmt,
										 .dbname = "target_database",
										 .options = &database_limit_options };
	Integer tablespace_cost_value = { .type = T_Integer, .ival = 1 };
	DefElem tablespace_option
		= { .type = T_DefElem, .defname = "seq_page_cost", .arg = (Node *)&tablespace_cost_value };
	ListCell tablespace_option_cell = { .ptr_value = &tablespace_option };
	List tablespace_option_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &tablespace_option_cell };
	AlterTableSpaceOptionsStmt tablespace_options = { .type = T_AlterTableSpaceOptionsStmt,
													  .tablespacename = "g_preflight_tablespace",
													  .options = &tablespace_option_list };
	String label_name = { .type = T_String, .sval = "label_target" };
	String label_column_name = { .type = T_String, .sval = "label_column" };
	ListCell label_name_cell = { .ptr_value = &label_name };
	List label_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &label_name_cell };
	ListCell label_column_cells[2]
		= { { .ptr_value = &label_name }, { .ptr_value = &label_column_name } };
	List label_column_names
		= { .type = T_List, .length = 2, .max_length = 2, .elements = label_column_cells };
	ObjectWithArgs label_routine = { .type = T_ObjectWithArgs, .objname = &label_names };
	SecLabelStmt label_table = { .type = T_SecLabelStmt,
								 .objtype = OBJECT_TABLE,
								 .object = (Node *)&label_names,
								 .provider = "g_label_provider",
								 .label = "table_label" };
	SecLabelStmt label_column = { .type = T_SecLabelStmt,
								  .objtype = OBJECT_COLUMN,
								  .object = (Node *)&label_column_names };
	SecLabelStmt label_function = { .type = T_SecLabelStmt,
									.objtype = OBJECT_FUNCTION,
									.object = (Node *)&label_routine,
									.provider = "g_label_provider",
									.label = "routine_label" };
	SecLabelStmt label_database
		= { .type = T_SecLabelStmt, .objtype = OBJECT_DATABASE, .object = (Node *)&label_name };
	RangeVar static_relation = { .type = T_RangeVar,
								 .relname = "alter_target",
								 .inh = true,
								 .relpersistence = RELPERSISTENCE_PERMANENT };
	Integer static_stats_value = { .type = T_Integer, .ival = 100 };
	AlterTableCmd static_stats_cmd = { .type = T_AlterTableCmd,
									   .subtype = AT_SetStatistics,
									   .name = "target_column",
									   .def = (Node *)&static_stats_value };
	RoleSpec static_owner_role
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	AlterTableCmd static_owner_cmd
		= { .type = T_AlterTableCmd, .subtype = AT_ChangeOwner, .newowner = &static_owner_role };
	Integer static_fillfactor = { .type = T_Integer, .ival = 90 };
	DefElem static_set_option
		= { .type = T_DefElem, .defname = "fillfactor", .arg = (Node *)&static_fillfactor };
	DefElem static_reset_option = { .type = T_DefElem, .defname = "autovacuum_enabled" };
	ListCell static_set_cell = { .ptr_value = &static_set_option };
	ListCell static_reset_cell = { .ptr_value = &static_reset_option };
	List static_set_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_set_cell };
	List static_reset_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_reset_cell };
	AlterTableCmd static_set_cmd
		= { .type = T_AlterTableCmd, .subtype = AT_SetRelOptions, .def = (Node *)&static_set_list };
	AlterTableCmd static_reset_cmd = { .type = T_AlterTableCmd,
									   .subtype = AT_ResetRelOptions,
									   .def = (Node *)&static_reset_list };
	ListCell static_stats_cell = { .ptr_value = &static_stats_cmd };
	ListCell static_owner_cell = { .ptr_value = &static_owner_cmd };
	ListCell static_option_cells[2]
		= { { .ptr_value = &static_set_cmd }, { .ptr_value = &static_reset_cmd } };
	List static_stats_cmds
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_stats_cell };
	List static_owner_cmds
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_owner_cell };
	List static_option_cmds
		= { .type = T_List, .length = 2, .max_length = 2, .elements = static_option_cells };
	AlterTableStmt static_statistics = { .type = T_AlterTableStmt,
										 .objtype = OBJECT_TABLE,
										 .relation = &static_relation,
										 .cmds = &static_stats_cmds };
	AlterTableStmt static_owner = { .type = T_AlterTableStmt,
									.objtype = OBJECT_TABLE,
									.relation = &static_relation,
									.cmds = &static_owner_cmds };
	AlterTableStmt static_options = { .type = T_AlterTableStmt,
									  .objtype = OBJECT_TABLE,
									  .relation = &static_relation,
									  .cmds = &static_option_cmds };
	RangeVar import_first = { .type = T_RangeVar, .relname = "import_first", .location = -1 };
	RangeVar import_second = { .type = T_RangeVar, .relname = "import_second", .location = -1 };
	ListCell import_table_cells[2]
		= { { .ptr_value = &import_first }, { .ptr_value = &import_second } };
	List import_tables
		= { .type = T_List, .length = 2, .max_length = 2, .elements = import_table_cells };
	String import_option_value = { .type = T_String, .sval = "true" };
	DefElem import_option
		= { .type = T_DefElem, .defname = "import_default", .arg = (Node *)&import_option_value };
	ListCell import_option_cell = { .ptr_value = &import_option };
	List import_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &import_option_cell };
	ImportForeignSchemaStmt import_all = { .type = T_ImportForeignSchemaStmt,
										   .server_name = "target_server",
										   .remote_schema = "remote_schema",
										   .local_schema = "local_schema",
										   .list_type = FDW_IMPORT_SCHEMA_ALL };
	ImportForeignSchemaStmt import_limit = { .type = T_ImportForeignSchemaStmt,
											 .server_name = "target_server",
											 .remote_schema = "remote_schema",
											 .local_schema = "local_schema",
											 .list_type = FDW_IMPORT_SCHEMA_LIMIT_TO,
											 .table_list = &import_tables,
											 .options = &import_options };
	ImportForeignSchemaStmt import_except = { .type = T_ImportForeignSchemaStmt,
											  .server_name = "target_server",
											  .remote_schema = "remote_schema",
											  .local_schema = "local_schema",
											  .list_type = FDW_IMPORT_SCHEMA_EXCEPT,
											  .table_list = &import_tables,
											  .options = &import_options };
	AlterExtensionContentsStmt extension_add_table = { .type = T_AlterExtensionContentsStmt,
													   .extname = "existing_extension",
													   .action = 1,
													   .objtype = OBJECT_TABLE,
													   .object = (Node *)&dependency_names };
	AlterExtensionContentsStmt extension_drop_table = { .type = T_AlterExtensionContentsStmt,
														.extname = "existing_extension",
														.action = -1,
														.objtype = OBJECT_TABLE,
														.object = (Node *)&dependency_names };
	AlterExtensionContentsStmt extension_add_function = { .type = T_AlterExtensionContentsStmt,
														  .extname = "existing_extension",
														  .action = 1,
														  .objtype = OBJECT_FUNCTION,
														  .object = (Node *)&dependency_routine };
	AlterExtensionContentsStmt extension_drop_function = { .type = T_AlterExtensionContentsStmt,
														   .extname = "existing_extension",
														   .action = -1,
														   .objtype = OBJECT_FUNCTION,
														   .object = (Node *)&dependency_routine };
	String extension_version = { .type = T_String, .sval = "2.0" };
	DefElem version_option
		= { .type = T_DefElem, .defname = "new_version", .arg = (Node *)&extension_version };
	ListCell version_cell = { .ptr_value = &version_option };
	List version_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &version_cell };
	AlterExtensionStmt extension_default
		= { .type = T_AlterExtensionStmt, .extname = "existing_extension" };
	AlterExtensionStmt extension_explicit = { .type = T_AlterExtensionStmt,
											  .extname = "existing_extension",
											  .options = &version_options };
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
						   (Node *)&access_method,
						   (Node *)&create_fdw,
						   (Node *)&alter_fdw,
						   (Node *)&create_server,
						   (Node *)&alter_server,
						   (Node *)&create_mapping,
						   (Node *)&alter_mapping,
						   (Node *)&drop_mapping,
						   (Node *)&create_publication,
						   (Node *)&create_publication_objects,
						   (Node *)&alter_publication_options,
						   (Node *)&publication_add,
						   (Node *)&publication_drop,
						   (Node *)&publication_set,
						   (Node *)&depends_function,
						   (Node *)&depends_procedure,
						   (Node *)&depends_routine,
						   (Node *)&depends_trigger,
						   (Node *)&depends_index,
						   (Node *)&reassign_named,
						   (Node *)&reassign_context,
						   (Node *)&foreign_table,
						   (Node *)&owner_database,
						   (Node *)&owner_schema,
						   (Node *)&owner_function,
						   (Node *)&owner_collation,
						   (Node *)&owner_tablespace,
						   (Node *)&database_flags,
						   (Node *)&database_limit,
						   (Node *)&tablespace_options,
						   (Node *)&label_table,
						   (Node *)&label_column,
						   (Node *)&label_function,
						   (Node *)&label_database,
						   (Node *)&static_statistics,
						   (Node *)&static_owner,
						   (Node *)&static_options,
						   (Node *)&import_all,
						   (Node *)&import_limit,
						   (Node *)&import_except,
						   (Node *)&extension_add_table,
						   (Node *)&extension_drop_table,
						   (Node *)&extension_add_function,
						   (Node *)&extension_drop_function,
						   (Node *)&extension_default,
						   (Node *)&extension_explicit };

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
	DefElem no_validator = { .type = T_DefElem, .defname = "validator" };
	ListCell validator_cell = { .ptr_value = &no_validator };
	List validator_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &validator_cell };
	CreateFdwStmt create_fdw
		= { .type = T_CreateFdwStmt, .fdwname = "target_fdw", .func_options = &validator_options };
	AlterFdwStmt alter_fdw
		= { .type = T_AlterFdwStmt, .fdwname = "target_fdw", .func_options = &validator_options };
	CreateForeignServerStmt create_server = { .type = T_CreateForeignServerStmt,
											  .servername = "target_server",
											  .fdwname = "target_fdw" };
	AlterForeignServerStmt alter_server = { .type = T_AlterForeignServerStmt,
											.servername = "target_server",
											.version = "preflight",
											.has_version = true };
	RoleSpec mapping_user = { .type = T_RoleSpec, .roletype = ROLESPEC_CURRENT_USER };
	CreateUserMappingStmt create_mapping
		= { .type = T_CreateUserMappingStmt, .user = &mapping_user, .servername = "target_server" };
	String mapping_option_value = { .type = T_String, .sval = "enabled" };
	DefElem mapping_option = { .type = T_DefElem,
							   .defname = "g_preflight",
							   .arg = (Node *)&mapping_option_value,
							   .defaction = DEFELEM_ADD };
	ListCell mapping_cell = { .ptr_value = &mapping_option };
	List mapping_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &mapping_cell };
	AlterUserMappingStmt alter_mapping = { .type = T_AlterUserMappingStmt,
										   .user = &mapping_user,
										   .servername = "target_server",
										   .options = &mapping_options };
	DropUserMappingStmt drop_mapping
		= { .type = T_DropUserMappingStmt, .user = &mapping_user, .servername = "target_server" };
	String publish_value = { .type = T_String, .sval = "insert" };
	DefElem publish_option
		= { .type = T_DefElem, .defname = "publish", .arg = (Node *)&publish_value };
	ListCell publish_option_cell = { .ptr_value = &publish_option };
	List publish_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &publish_option_cell };
	PublicationTable publication_table
		= { .type = T_PublicationTable, .relation = &stats_relation };
	PublicationObjSpec publication_table_spec = { .type = T_PublicationObjSpec,
												  .pubobjtype = PUBLICATIONOBJ_TABLE,
												  .pubtable = &publication_table };
	PublicationObjSpec publication_schema_spec = { .type = T_PublicationObjSpec,
												   .pubobjtype = PUBLICATIONOBJ_TABLES_IN_SCHEMA,
												   .name = "target_schema" };
	ListCell publication_cells[2]
		= { { .ptr_value = &publication_table_spec }, { .ptr_value = &publication_schema_spec } };
	List publication_objects
		= { .type = T_List, .length = 2, .max_length = 2, .elements = publication_cells };
	CreatePublicationStmt create_publication = { .type = T_CreatePublicationStmt,
												 .pubname = "target_publication",
												 .options = &publish_options };
	CreatePublicationStmt create_publication_objects = { .type = T_CreatePublicationStmt,
														 .pubname = "target_publication",
														 .pubobjects = &publication_objects };
	AlterPublicationStmt alter_publication_options = { .type = T_AlterPublicationStmt,
													   .pubname = "target_publication",
													   .options = &publish_options };
	AlterPublicationStmt publication_add = { .type = T_AlterPublicationStmt,
											 .pubname = "target_publication",
											 .pubobjects = &publication_objects,
											 .action = AP_AddObjects };
	AlterPublicationStmt publication_drop = { .type = T_AlterPublicationStmt,
											  .pubname = "target_publication",
											  .pubobjects = &publication_objects,
											  .action = AP_DropObjects };
	AlterPublicationStmt publication_set = { .type = T_AlterPublicationStmt,
											 .pubname = "target_publication",
											 .pubobjects = &publication_objects,
											 .action = AP_SetObjects };
	String dependency_name = { .type = T_String, .sval = "target_object" };
	ListCell dependency_name_cell = { .ptr_value = &dependency_name };
	List dependency_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &dependency_name_cell };
	ObjectWithArgs dependency_routine = { .type = T_ObjectWithArgs, .objname = &dependency_names };
	String dependency_extension = { .type = T_String, .sval = "target_extension" };
	AlterObjectDependsStmt depends_function = { .type = T_AlterObjectDependsStmt,
												.objectType = OBJECT_FUNCTION,
												.object = (Node *)&dependency_routine,
												.extname = &dependency_extension };
	AlterObjectDependsStmt depends_procedure = { .type = T_AlterObjectDependsStmt,
												 .objectType = OBJECT_PROCEDURE,
												 .object = (Node *)&dependency_routine,
												 .extname = &dependency_extension };
	AlterObjectDependsStmt depends_routine = { .type = T_AlterObjectDependsStmt,
											   .objectType = OBJECT_ROUTINE,
											   .object = (Node *)&dependency_routine,
											   .extname = &dependency_extension };
	AlterObjectDependsStmt depends_trigger = { .type = T_AlterObjectDependsStmt,
											   .objectType = OBJECT_TRIGGER,
											   .relation = &stats_relation,
											   .object = (Node *)&dependency_names,
											   .extname = &dependency_extension };
	AlterObjectDependsStmt depends_index = { .type = T_AlterObjectDependsStmt,
											 .objectType = OBJECT_INDEX,
											 .relation = &stats_relation,
											 .extname = &dependency_extension };
	RoleSpec reassign_first
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "source_owner" };
	RoleSpec reassign_second
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "second_owner" };
	RoleSpec reassign_receiver
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	ListCell reassign_cells[2]
		= { { .ptr_value = &reassign_first }, { .ptr_value = &reassign_second } };
	List reassign_roles
		= { .type = T_List, .length = 2, .max_length = 2, .elements = reassign_cells };
	ReassignOwnedStmt reassign_named
		= { .type = T_ReassignOwnedStmt, .roles = &reassign_roles, .newrole = &reassign_receiver };
	RoleSpec reassign_self = { .type = T_RoleSpec, .roletype = ROLESPEC_CURRENT_USER };
	ListCell reassign_self_cell = { .ptr_value = &reassign_self };
	List reassign_self_roles
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &reassign_self_cell };
	ReassignOwnedStmt reassign_context
		= { .type = T_ReassignOwnedStmt, .roles = &reassign_self_roles, .newrole = &reassign_self };
	RangeVar foreign_relation = { .type = T_RangeVar,
								  .schemaname = "g_preflight",
								  .relname = "foreign_target",
								  .relpersistence = RELPERSISTENCE_PERMANENT };
	String foreign_option_value = { .type = T_String, .sval = "preflight" };
	DefElem foreign_option
		= { .type = T_DefElem, .defname = "g_preflight", .arg = (Node *)&foreign_option_value };
	ListCell foreign_option_cell = { .ptr_value = &foreign_option };
	List foreign_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &foreign_option_cell };
	CreateForeignTableStmt foreign_table = { .base = { .type = T_CreateForeignTableStmt,
													   .relation = &foreign_relation,
													   .oncommit = ONCOMMIT_NOOP },
											 .servername = "target_server",
											 .options = &foreign_options };
	RoleSpec owner_receiver
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	String owner_target = { .type = T_String, .sval = "owner_target" };
	String owner_schema_name = { .type = T_String, .sval = "pg_undo" };
	String owner_space_name = { .type = T_String, .sval = "g_preflight_tablespace" };
	AlterOwnerStmt owner_database = { .type = T_AlterOwnerStmt,
									  .objectType = OBJECT_DATABASE,
									  .object = (Node *)&owner_target,
									  .newowner = &owner_receiver };
	AlterOwnerStmt owner_schema = { .type = T_AlterOwnerStmt,
									.objectType = OBJECT_SCHEMA,
									.object = (Node *)&owner_schema_name,
									.newowner = &reassign_self };
	AlterOwnerStmt owner_function = { .type = T_AlterOwnerStmt,
									  .objectType = OBJECT_FUNCTION,
									  .object = (Node *)&dependency_routine,
									  .newowner = &owner_receiver };
	AlterOwnerStmt owner_collation = { .type = T_AlterOwnerStmt,
									   .objectType = OBJECT_COLLATION,
									   .object = (Node *)&dependency_names,
									   .newowner = &owner_receiver };
	AlterOwnerStmt owner_tablespace = { .type = T_AlterOwnerStmt,
										.objectType = OBJECT_TABLESPACE,
										.object = (Node *)&owner_space_name,
										.newowner = &owner_receiver };
	String database_template_value = { .type = T_String, .sval = "false" };
	String database_allow_value = { .type = T_String, .sval = "true" };
	Integer database_limit_value = { .type = T_Integer, .ival = 100 };
	DefElem database_template_option
		= { .type = T_DefElem, .defname = "is_template", .arg = (Node *)&database_template_value };
	DefElem database_allow_option = { .type = T_DefElem,
									  .defname = "allow_connections",
									  .arg = (Node *)&database_allow_value };
	DefElem database_limit_option = { .type = T_DefElem,
									  .defname = "connection_limit",
									  .arg = (Node *)&database_limit_value };
	ListCell database_flags_cells[2]
		= { { .ptr_value = &database_template_option }, { .ptr_value = &database_allow_option } };
	List database_flags_options
		= { .type = T_List, .length = 2, .max_length = 2, .elements = database_flags_cells };
	ListCell database_limit_cell = { .ptr_value = &database_limit_option };
	List database_limit_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &database_limit_cell };
	AlterDatabaseStmt database_flags = { .type = T_AlterDatabaseStmt,
										 .dbname = "target_database",
										 .options = &database_flags_options };
	AlterDatabaseStmt database_limit = { .type = T_AlterDatabaseStmt,
										 .dbname = "target_database",
										 .options = &database_limit_options };
	Integer tablespace_cost_value = { .type = T_Integer, .ival = 1 };
	DefElem tablespace_option
		= { .type = T_DefElem, .defname = "seq_page_cost", .arg = (Node *)&tablespace_cost_value };
	ListCell tablespace_option_cell = { .ptr_value = &tablespace_option };
	List tablespace_option_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &tablespace_option_cell };
	AlterTableSpaceOptionsStmt tablespace_options = { .type = T_AlterTableSpaceOptionsStmt,
													  .tablespacename = "g_preflight_tablespace",
													  .options = &tablespace_option_list };
	String label_name = { .type = T_String, .sval = "label_target" };
	String label_column_name = { .type = T_String, .sval = "label_column" };
	ListCell label_name_cell = { .ptr_value = &label_name };
	List label_names
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &label_name_cell };
	ListCell label_column_cells[2]
		= { { .ptr_value = &label_name }, { .ptr_value = &label_column_name } };
	List label_column_names
		= { .type = T_List, .length = 2, .max_length = 2, .elements = label_column_cells };
	ObjectWithArgs label_routine = { .type = T_ObjectWithArgs, .objname = &label_names };
	SecLabelStmt label_table = { .type = T_SecLabelStmt,
								 .objtype = OBJECT_TABLE,
								 .object = (Node *)&label_names,
								 .provider = "g_label_provider",
								 .label = "table_label" };
	SecLabelStmt label_column = { .type = T_SecLabelStmt,
								  .objtype = OBJECT_COLUMN,
								  .object = (Node *)&label_column_names };
	SecLabelStmt label_function = { .type = T_SecLabelStmt,
									.objtype = OBJECT_FUNCTION,
									.object = (Node *)&label_routine,
									.provider = "g_label_provider",
									.label = "routine_label" };
	SecLabelStmt label_database
		= { .type = T_SecLabelStmt, .objtype = OBJECT_DATABASE, .object = (Node *)&label_name };
	RangeVar static_relation = { .type = T_RangeVar,
								 .relname = "alter_target",
								 .inh = true,
								 .relpersistence = RELPERSISTENCE_PERMANENT };
	Integer static_stats_value = { .type = T_Integer, .ival = 100 };
	AlterTableCmd static_stats_cmd = { .type = T_AlterTableCmd,
									   .subtype = AT_SetStatistics,
									   .name = "target_column",
									   .def = (Node *)&static_stats_value };
	RoleSpec static_owner_role
		= { .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "receiving_owner" };
	AlterTableCmd static_owner_cmd
		= { .type = T_AlterTableCmd, .subtype = AT_ChangeOwner, .newowner = &static_owner_role };
	Integer static_fillfactor = { .type = T_Integer, .ival = 90 };
	DefElem static_set_option
		= { .type = T_DefElem, .defname = "fillfactor", .arg = (Node *)&static_fillfactor };
	DefElem static_reset_option = { .type = T_DefElem, .defname = "autovacuum_enabled" };
	ListCell static_set_cell = { .ptr_value = &static_set_option };
	ListCell static_reset_cell = { .ptr_value = &static_reset_option };
	List static_set_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_set_cell };
	List static_reset_list
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_reset_cell };
	AlterTableCmd static_set_cmd
		= { .type = T_AlterTableCmd, .subtype = AT_SetRelOptions, .def = (Node *)&static_set_list };
	AlterTableCmd static_reset_cmd = { .type = T_AlterTableCmd,
									   .subtype = AT_ResetRelOptions,
									   .def = (Node *)&static_reset_list };
	ListCell static_stats_cell = { .ptr_value = &static_stats_cmd };
	ListCell static_owner_cell = { .ptr_value = &static_owner_cmd };
	ListCell static_option_cells[2]
		= { { .ptr_value = &static_set_cmd }, { .ptr_value = &static_reset_cmd } };
	List static_stats_cmds
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_stats_cell };
	List static_owner_cmds
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &static_owner_cell };
	List static_option_cmds
		= { .type = T_List, .length = 2, .max_length = 2, .elements = static_option_cells };
	AlterTableStmt static_statistics = { .type = T_AlterTableStmt,
										 .objtype = OBJECT_TABLE,
										 .relation = &static_relation,
										 .cmds = &static_stats_cmds };
	AlterTableStmt static_owner = { .type = T_AlterTableStmt,
									.objtype = OBJECT_TABLE,
									.relation = &static_relation,
									.cmds = &static_owner_cmds };
	AlterTableStmt static_options = { .type = T_AlterTableStmt,
									  .objtype = OBJECT_TABLE,
									  .relation = &static_relation,
									  .cmds = &static_option_cmds };
	RangeVar import_first = { .type = T_RangeVar, .relname = "import_first", .location = -1 };
	RangeVar import_second = { .type = T_RangeVar, .relname = "import_second", .location = -1 };
	ListCell import_table_cells[2]
		= { { .ptr_value = &import_first }, { .ptr_value = &import_second } };
	List import_tables
		= { .type = T_List, .length = 2, .max_length = 2, .elements = import_table_cells };
	String import_option_value = { .type = T_String, .sval = "true" };
	DefElem import_option
		= { .type = T_DefElem, .defname = "import_default", .arg = (Node *)&import_option_value };
	ListCell import_option_cell = { .ptr_value = &import_option };
	List import_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &import_option_cell };
	ImportForeignSchemaStmt import_all = { .type = T_ImportForeignSchemaStmt,
										   .server_name = "target_server",
										   .remote_schema = "remote_schema",
										   .local_schema = "local_schema",
										   .list_type = FDW_IMPORT_SCHEMA_ALL };
	ImportForeignSchemaStmt import_limit = { .type = T_ImportForeignSchemaStmt,
											 .server_name = "target_server",
											 .remote_schema = "remote_schema",
											 .local_schema = "local_schema",
											 .list_type = FDW_IMPORT_SCHEMA_LIMIT_TO,
											 .table_list = &import_tables,
											 .options = &import_options };
	ImportForeignSchemaStmt import_except = { .type = T_ImportForeignSchemaStmt,
											  .server_name = "target_server",
											  .remote_schema = "remote_schema",
											  .local_schema = "local_schema",
											  .list_type = FDW_IMPORT_SCHEMA_EXCEPT,
											  .table_list = &import_tables,
											  .options = &import_options };
	AlterExtensionContentsStmt extension_add_table = { .type = T_AlterExtensionContentsStmt,
													   .extname = "existing_extension",
													   .action = 1,
													   .objtype = OBJECT_TABLE,
													   .object = (Node *)&dependency_names };
	AlterExtensionContentsStmt extension_drop_table = { .type = T_AlterExtensionContentsStmt,
														.extname = "existing_extension",
														.action = -1,
														.objtype = OBJECT_TABLE,
														.object = (Node *)&dependency_names };
	AlterExtensionContentsStmt extension_add_function = { .type = T_AlterExtensionContentsStmt,
														  .extname = "existing_extension",
														  .action = 1,
														  .objtype = OBJECT_FUNCTION,
														  .object = (Node *)&dependency_routine };
	AlterExtensionContentsStmt extension_drop_function = { .type = T_AlterExtensionContentsStmt,
														   .extname = "existing_extension",
														   .action = -1,
														   .objtype = OBJECT_FUNCTION,
														   .object = (Node *)&dependency_routine };
	String extension_version = { .type = T_String, .sval = "2.0" };
	DefElem version_option
		= { .type = T_DefElem, .defname = "new_version", .arg = (Node *)&extension_version };
	ListCell version_cell = { .ptr_value = &version_option };
	List version_options
		= { .type = T_List, .length = 1, .max_length = 1, .elements = &version_cell };
	AlterExtensionStmt extension_default
		= { .type = T_AlterExtensionStmt, .extname = "existing_extension" };
	AlterExtensionStmt extension_explicit = { .type = T_AlterExtensionStmt,
											  .extname = "existing_extension",
											  .options = &version_options };
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
						   (Node *)&access_method,
						   (Node *)&create_fdw,
						   (Node *)&alter_fdw,
						   (Node *)&create_server,
						   (Node *)&alter_server,
						   (Node *)&create_mapping,
						   (Node *)&alter_mapping,
						   (Node *)&drop_mapping,
						   (Node *)&create_publication,
						   (Node *)&create_publication_objects,
						   (Node *)&alter_publication_options,
						   (Node *)&publication_add,
						   (Node *)&publication_drop,
						   (Node *)&publication_set,
						   (Node *)&depends_function,
						   (Node *)&depends_procedure,
						   (Node *)&depends_routine,
						   (Node *)&depends_trigger,
						   (Node *)&depends_index,
						   (Node *)&reassign_named,
						   (Node *)&reassign_context,
						   (Node *)&foreign_table,
						   (Node *)&owner_database,
						   (Node *)&owner_schema,
						   (Node *)&owner_function,
						   (Node *)&owner_collation,
						   (Node *)&owner_tablespace,
						   (Node *)&database_flags,
						   (Node *)&database_limit,
						   (Node *)&tablespace_options,
						   (Node *)&label_table,
						   (Node *)&label_column,
						   (Node *)&label_function,
						   (Node *)&label_database,
						   (Node *)&static_statistics,
						   (Node *)&static_owner,
						   (Node *)&static_options,
						   (Node *)&import_all,
						   (Node *)&import_limit,
						   (Node *)&import_except,
						   (Node *)&extension_add_table,
						   (Node *)&extension_drop_table,
						   (Node *)&extension_add_function,
						   (Node *)&extension_drop_function,
						   (Node *)&extension_default,
						   (Node *)&extension_explicit };
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
						   "CREATE ACCESS METHOD",
						   "CREATE FOREIGN DATA WRAPPER",
						   "ALTER FOREIGN DATA WRAPPER",
						   "CREATE SERVER",
						   "ALTER SERVER",
						   "CREATE USER MAPPING",
						   "ALTER USER MAPPING",
						   "DROP USER MAPPING",
						   "CREATE PUBLICATION",
						   "CREATE PUBLICATION",
						   "ALTER PUBLICATION",
						   "ALTER PUBLICATION",
						   "ALTER PUBLICATION",
						   "ALTER PUBLICATION",
						   "ALTER FUNCTION",
						   "ALTER PROCEDURE",
						   "ALTER ROUTINE",
						   "ALTER TRIGGER",
						   "ALTER INDEX",
						   "REASSIGN OWNED",
						   "REASSIGN OWNED",
						   "CREATE FOREIGN TABLE",
						   "ALTER DATABASE",
						   "ALTER SCHEMA",
						   "ALTER FUNCTION",
						   "ALTER COLLATION",
						   "ALTER TABLESPACE",
						   "ALTER DATABASE",
						   "ALTER DATABASE",
						   "ALTER TABLESPACE",
						   "SECURITY LABEL",
						   "SECURITY LABEL",
						   "SECURITY LABEL",
						   "SECURITY LABEL",
						   "ALTER TABLE",
						   "ALTER TABLE",
						   "ALTER TABLE",
						   "IMPORT FOREIGN SCHEMA",
						   "IMPORT FOREIGN SCHEMA",
						   "IMPORT FOREIGN SCHEMA",
						   "ALTER EXTENSION",
						   "ALTER EXTENSION",
						   "ALTER EXTENSION",
						   "ALTER EXTENSION",
						   "ALTER EXTENSION",
						   "ALTER EXTENSION" };

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

UT_TEST(test_materialized_view_dependency_refuses_before_binding)
{
	RangeVar relation = { .type = T_RangeVar, .relname = "target_matview" };
	String extension = { .type = T_String, .sval = "target_extension" };
	AlterObjectDependsStmt depends = { .type = T_AlterObjectDependsStmt,
									   .objectType = OBJECT_MATVIEW,
									   .relation = &relation,
									   .extname = &extension };
	ListCell child = { .ptr_value = &depends };
	List children = { .type = T_List, .length = 1, .max_length = 1, .elements = &child };
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt, .schemaElts = &children };
	Node *statements[] = { (Node *)&depends, (Node *)&schema };

	/* The nested raw AST exercises support recursion, not parsed schema SQL. */
	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned remove = 0; remove < 2; remove++) {
			depends.remove = remove != 0;
			for (unsigned i = 0; i < lengthof(statements); i++) {
				int caught;

				reset_create_preflight();
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					create_sql_entry(statements[i]);
				UT_ASSERT_EQ(caught != 0, (mode & 5) != 0);
				if (caught)
					UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
				UT_ASSERT_EQ(utility_xids, 0);
				UT_ASSERT_EQ(utility_prepares, 0);
				UT_ASSERT_EQ(utility_binds, 0);
				UT_ASSERT_EQ(utility_after_preflight, (mode & 5) == 0);
			}
		}
	}
}

UT_TEST(test_foreign_table_unsupported_base_refuses_before_binding)
{
	RangeVar relation = { .type = T_RangeVar, .relname = "foreign_target" };
	RangeVar parent = { .type = T_RangeVar, .relname = "parent" };
	ListCell parent_cell = { .ptr_value = &parent };
	List parents = { .type = T_List, .length = 1, .max_length = 1, .elements = &parent_cell };
	PartitionBoundSpec partbound = { .type = T_PartitionBoundSpec, .is_default = true };
	CreateForeignTableStmt foreign_table = { .base = { .type = T_CreateForeignTableStmt,
													   .relation = &relation,
													   .oncommit = ONCOMMIT_NOOP },
											 .servername = "target_server" };
	ListCell schema_cell = { .ptr_value = &foreign_table };
	List children = { .type = T_List, .length = 1, .max_length = 1, .elements = &schema_cell };
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt, .schemaElts = &children };
	Node *statements[] = { (Node *)&foreign_table, (Node *)&schema };

	/* UNLOGGED and the nested schema are raw routing inputs, not parsed SQL. */
	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned variant = 0; variant < 3; variant++) {
			foreign_table.base.inhRelations = variant < 2 ? &parents : NIL;
			foreign_table.base.partbound = variant == 1 ? &partbound : NULL;
			relation.relpersistence
				= variant == 2 ? RELPERSISTENCE_UNLOGGED : RELPERSISTENCE_PERMANENT;
			for (unsigned child = 0; child < 2; child++) {
				for (unsigned i = 0; i < lengthof(statements); i++) {
					int caught;

					reset_create_preflight();
					cluster_shared_catalog = (mode & 1) != 0;
					utility_storage_mode = (mode & 2) != 0;
					cluster_shared_config = (mode & 4) != 0;
					utility_in_child = child != 0;
					caught = sigsetjmp(boundary, 1);
					if (!caught)
						create_sql_entry(statements[i]);
					UT_ASSERT_EQ(caught != 0, (mode & 5) != 0);
					if (caught) {
						UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
						UT_ASSERT(strstr(error_message,
										 variant == 2 ? "UNLOGGED tables"
													  : "partition and inheritance attachment")
								  != NULL);
					}
					UT_ASSERT_EQ(utility_xids, 0);
					UT_ASSERT_EQ(utility_prepares, 0);
					UT_ASSERT_EQ(utility_binds, 0);
					UT_ASSERT_EQ(utility_after_preflight, (mode & 5) == 0);
				}
			}
		}
	}
}

UT_TEST(test_undo_tablespace_owner_refuses_before_binding)
{
	String undo_name = { .type = T_String, .sval = "pg_undo" };
	RoleSpec receiver = { .type = T_RoleSpec, .rolename = "g_missing_owner" };
	AlterOwnerStmt owner = { .type = T_AlterOwnerStmt,
							 .objectType = OBJECT_TABLESPACE,
							 .object = (Node *)&undo_name,
							 .newowner = &receiver };
	ListCell child_cell = { .ptr_value = &owner };
	List children = { .type = T_List, .length = 1, .max_length = 1, .elements = &child_cell };
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt, .schemaElts = &children };
	Node *statements[] = { (Node *)&owner, (Node *)&schema };

	/* Synthetic schema recursion; reaching the native boundary outside
	 * shared scope does not bypass the original OID-based rejection. */
	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned child = 0; child < 2; child++) {
			receiver.roletype = child ? ROLESPEC_CSTRING : ROLESPEC_CURRENT_USER;
			for (unsigned i = 0; i < lengthof(statements); i++) {
				int caught;

				reset_create_preflight();
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				utility_in_child = child != 0;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					create_sql_entry(statements[i]);
				UT_ASSERT_EQ(caught != 0, (mode & 5) != 0);
				if (caught) {
					UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
					UT_ASSERT(strcmp(error_message, "pg_undo cannot be altered") == 0);
					UT_ASSERT(strcmp(error_hint, "pg_undo is a hard-coded system tablespace; owner "
												 "changes are not supported.")
							  == 0);
				}
				UT_ASSERT_EQ(utility_xids, 0);
				UT_ASSERT_EQ(utility_prepares, 0);
				UT_ASSERT_EQ(utility_binds, 0);
				UT_ASSERT_EQ(utility_after_preflight, (mode & 5) == 0);
			}
		}
	}
}

UT_TEST(test_undo_tablespace_options_refuses_before_binding)
{
	Integer cost = { .type = T_Integer, .ival = 1 };
	DefElem option = { .type = T_DefElem, .defname = "seq_page_cost" };
	ListCell option_cell = { .ptr_value = &option };
	List options = { .type = T_List, .length = 1, .max_length = 1, .elements = &option_cell };
	AlterTableSpaceOptionsStmt alter = { .type = T_AlterTableSpaceOptionsStmt,
										 .tablespacename = "pg_undo",
										 .options = &options };
	ListCell child_cell = { .ptr_value = &alter };
	List children = { .type = T_List, .length = 1, .max_length = 1, .elements = &child_cell };
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt, .schemaElts = &children };
	Node *statements[] = { (Node *)&alter, (Node *)&schema };

	/* Synthetic recursion; nonshared native boundary still has the OID gate. */
	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned reset = 0; reset < 2; reset++) {
			alter.isReset = reset != 0;
			option.arg = reset ? NULL : (Node *)&cost;
			for (unsigned child = 0; child < 2; child++) {
				for (unsigned i = 0; i < lengthof(statements); i++) {
					int caught;

					reset_create_preflight();
					cluster_shared_catalog = (mode & 1) != 0;
					utility_storage_mode = (mode & 2) != 0;
					cluster_shared_config = (mode & 4) != 0;
					utility_in_child = child != 0;
					caught = sigsetjmp(boundary, 1);
					if (!caught)
						create_sql_entry(statements[i]);
					UT_ASSERT_EQ(caught != 0, (mode & 5) != 0);
					if (caught) {
						UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
						UT_ASSERT(strcmp(error_message, "pg_undo cannot be altered") == 0);
						UT_ASSERT(strcmp(error_hint,
										 "pg_undo is managed by the cluster runtime; Stage "
										 "1.22 does not support modifying its options.")
								  == 0);
					}
					UT_ASSERT_EQ(utility_xids, 0);
					UT_ASSERT_EQ(utility_prepares, 0);
					UT_ASSERT_EQ(utility_binds, 0);
					UT_ASSERT_EQ(utility_after_preflight, (mode & 5) == 0);
				}
			}
		}
	}
}

UT_TEST(test_database_move_keeps_native_path_without_binding)
{
	String tablespace = { .type = T_String, .sval = "g_preflight_tablespace" };
	Integer limit = { .type = T_Integer, .ival = 100 };
	DefElem move = { .type = T_DefElem, .defname = "tablespace", .arg = (Node *)&tablespace };
	DefElem metadata = { .type = T_DefElem, .defname = "connection_limit", .arg = (Node *)&limit };
	ListCell move_cell = { .ptr_value = &move };
	ListCell move_first[2] = { { .ptr_value = &move }, { .ptr_value = &metadata } };
	ListCell move_last[2] = { { .ptr_value = &metadata }, { .ptr_value = &move } };
	List options[3] = { { .type = T_List, .length = 1, .max_length = 1, .elements = &move_cell },
						{ .type = T_List, .length = 2, .max_length = 2, .elements = move_first },
						{ .type = T_List, .length = 2, .max_length = 2, .elements = move_last } };
	AlterDatabaseStmt alter = { .type = T_AlterDatabaseStmt, .dbname = "target_database" };

	/* Native move/combination/transaction-block errors are beyond this boundary. */
	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned variant = 0; variant < lengthof(options); variant++) {
			for (unsigned child = 0; child < 2; child++) {
				int caught;

				alter.options = &options[variant];
				reset_create_preflight();
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				utility_in_child = utility_in_transaction = child != 0;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					create_sql_entry((Node *)&alter);
				UT_ASSERT_EQ(caught, 0);
				UT_ASSERT_EQ(utility_xids, 0);
				UT_ASSERT_EQ(utility_prepares, 0);
				UT_ASSERT_EQ(utility_binds, 0);
				UT_ASSERT_EQ(utility_after_preflight, 1);
			}
		}
	}
}

UT_TEST(test_unsupported_security_labels_refuse_before_binding)
{
	String target = { .type = T_String, .sval = "label_target" };
	ListCell target_cell = { .ptr_value = &target };
	List names = { .type = T_List, .length = 1, .max_length = 1, .elements = &target_cell };
	TypeName type_name = { .type = T_TypeName, .names = &names, .typemod = -1, .location = -1 };
	SecLabelStmt materialized
		= { .type = T_SecLabelStmt, .objtype = OBJECT_MATVIEW, .object = (Node *)&names };
	SecLabelStmt type_label
		= { .type = T_SecLabelStmt, .objtype = OBJECT_TYPE, .object = (Node *)&type_name };
	SecLabelStmt domain_label
		= { .type = T_SecLabelStmt, .objtype = OBJECT_DOMAIN, .object = (Node *)&type_name };
	SecLabelStmt *labels[] = { &materialized, &type_label, &domain_label };
	const char *messages[] = { "materialized views is not supported in shared mode",
							   "custom types is not supported in shared mode",
							   "custom types is not supported in shared mode" };
	ListCell child_cell = { .ptr_value = &materialized };
	List children = { .type = T_List, .length = 1, .max_length = 1, .elements = &child_cell };
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt, .schemaElts = &children };

	/* Nested schema is a raw support-recursion input, not parsed label SQL. */
	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned kind = 0; kind < lengthof(labels); kind++) {
			for (unsigned remove = 0; remove < 2; remove++) {
				labels[kind]->provider = remove ? NULL : "g_label_provider";
				labels[kind]->label = remove ? NULL : "new_label";
				child_cell.ptr_value = labels[kind];
				for (unsigned child = 0; child < 2; child++) {
					for (unsigned nested = 0; nested < 2; nested++) {
						int caught;
						Node *statement = nested ? (Node *)&schema : (Node *)labels[kind];

						reset_create_preflight();
						cluster_shared_catalog = (mode & 1) != 0;
						utility_storage_mode = (mode & 2) != 0;
						cluster_shared_config = (mode & 4) != 0;
						utility_in_child = child != 0;
						caught = sigsetjmp(boundary, 1);
						if (!caught)
							create_sql_entry(statement);
						UT_ASSERT_EQ(caught != 0, (mode & 5) != 0);
						if (caught) {
							UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
							UT_ASSERT(strcmp(error_message, messages[kind]) == 0);
						}
						UT_ASSERT_EQ(utility_xids, 0);
						UT_ASSERT_EQ(utility_prepares, 0);
						UT_ASSERT_EQ(utility_binds, 0);
						UT_ASSERT_EQ(utility_after_preflight, (mode & 5) == 0);
					}
				}
			}
		}
	}
}

UT_TEST(test_alter_rewrite_candidates_keep_native_path_without_binding)
{
	RangeVar relation = { .type = T_RangeVar,
						  .relname = "alter_target",
						  .inh = true,
						  .relpersistence = RELPERSISTENCE_PERMANENT };
	String type_name = { .type = T_String, .sval = "int4" };
	ListCell type_cell = { .ptr_value = &type_name };
	List type_names = { .type = T_List, .length = 1, .max_length = 1, .elements = &type_cell };
	TypeName type = { .type = T_TypeName, .names = &type_names, .typemod = -1, .location = -1 };
	ColumnDef add_column
		= { .type = T_ColumnDef, .colname = "new_column", .typeName = &type, .location = -1 };
	ColumnDef type_column = { .type = T_ColumnDef, .typeName = &type, .location = -1 };
	AlterTableCmd candidates[]
		= { { .type = T_AlterTableCmd, .subtype = AT_AddColumn, .def = (Node *)&add_column },
			{ .type = T_AlterTableCmd,
			  .subtype = AT_AlterColumnType,
			  .name = "target_column",
			  .def = (Node *)&type_column },
			{ .type = T_AlterTableCmd, .subtype = AT_SetAccessMethod, .name = "heap" },
			{ .type = T_AlterTableCmd, .subtype = AT_SetLogged } };
	Integer stats_value = { .type = T_Integer, .ival = 100 };
	AlterTableCmd metadata = { .type = T_AlterTableCmd,
							   .subtype = AT_SetStatistics,
							   .name = "target_column",
							   .def = (Node *)&stats_value };
	ListCell candidate_cell = { .ptr_value = &candidates[0] };
	ListCell metadata_first[2] = { { .ptr_value = &metadata }, { .ptr_value = &candidates[0] } };
	ListCell metadata_last[2] = { { .ptr_value = &candidates[0] }, { .ptr_value = &metadata } };
	List command_lists[]
		= { { .type = T_List, .length = 1, .max_length = 1, .elements = &candidate_cell },
			{ .type = T_List, .length = 2, .max_length = 2, .elements = metadata_first },
			{ .type = T_List, .length = 2, .max_length = 2, .elements = metadata_last } };
	AlterTableStmt alter
		= { .type = T_AlterTableStmt, .objtype = OBJECT_TABLE, .relation = &relation };

	/* Native dynamic precheck/locks/DDL are explicit boundaries in this fixture. */
	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned kind = 0; kind < lengthof(candidates); kind++) {
			candidate_cell.ptr_value = metadata_first[1].ptr_value = metadata_last[0].ptr_value
				= &candidates[kind];
			for (unsigned variant = 0; variant < lengthof(command_lists); variant++) {
				alter.cmds = &command_lists[variant];
				for (unsigned skip = 0; skip < 2; skip++) {
					alter.missing_ok = skip != 0;
					candidates[kind].missing_ok = kind == 0 && skip != 0;
					relation.inh = skip == 0;
					for (unsigned child = 0; child < 2; child++) {
						int caught;

						reset_create_preflight();
						cluster_shared_catalog = (mode & 1) != 0;
						utility_storage_mode = (mode & 2) != 0;
						cluster_shared_config = (mode & 4) != 0;
						utility_in_child = utility_in_transaction = child != 0;
						caught = sigsetjmp(boundary, 1);
						if (!caught)
							create_sql_entry((Node *)&alter);
						UT_ASSERT_EQ(caught, 0);
						UT_ASSERT_EQ(utility_xids, 0);
						UT_ASSERT_EQ(utility_prepares, 0);
						UT_ASSERT_EQ(utility_binds, 0);
						UT_ASSERT_EQ(utility_after_preflight, 1);
					}
				}
			}
		}
	}
}

UT_TEST(test_unsupported_alter_table_refuses_before_binding)
{
	RangeVar relation = { .type = T_RangeVar,
						  .relname = "alter_target",
						  .inh = true,
						  .relpersistence = RELPERSISTENCE_PERMANENT };
	RangeVar parent = { .type = T_RangeVar, .relname = "parent_target" };
	PartitionBoundSpec bound = { .type = T_PartitionBoundSpec, .is_default = true };
	PartitionCmd partition = { .type = T_PartitionCmd, .name = &parent, .bound = &bound };
	Integer stats_value = { .type = T_Integer, .ival = 100 };
	AlterTableCmd commands[]
		= { { .type = T_AlterTableCmd, .subtype = AT_SetUnLogged },
			{ .type = T_AlterTableCmd, .subtype = AT_SetTableSpace, .name = "target_space" },
			{ .type = T_AlterTableCmd, .subtype = AT_AddInherit, .def = (Node *)&parent },
			{ .type = T_AlterTableCmd, .subtype = AT_DropInherit, .def = (Node *)&parent },
			{ .type = T_AlterTableCmd, .subtype = AT_AttachPartition, .def = (Node *)&partition },
			{ .type = T_AlterTableCmd, .subtype = AT_DetachPartition, .def = (Node *)&partition },
			{ .type = T_AlterTableCmd,
			  .subtype = AT_DetachPartitionFinalize,
			  .def = (Node *)&partition },
			{ .type = T_AlterTableCmd,
			  .subtype = AT_SetStatistics,
			  .name = "target_column",
			  .def = (Node *)&stats_value } };
	ListCell command_cell = { .ptr_value = &commands[0] };
	List cmds = { .type = T_List, .length = 1, .max_length = 1, .elements = &command_cell };
	AlterTableStmt alter = { .type = T_AlterTableStmt,
							 .objtype = OBJECT_TABLE,
							 .relation = &relation,
							 .cmds = &cmds };
	ListCell schema_cell = { .ptr_value = &alter };
	List children = { .type = T_List, .length = 1, .max_length = 1, .elements = &schema_cell };
	CreateSchemaStmt schema = { .type = T_CreateSchemaStmt, .schemaElts = &children };
	Node *statements[] = { (Node *)&alter, (Node *)&schema };

	/* Synthetic schema only exercises the original raw support recursion. */
	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned kind = 0; kind < lengthof(commands); kind++) {
			command_cell.ptr_value = &commands[kind];
			alter.objtype = kind == lengthof(commands) - 1 ? OBJECT_MATVIEW : OBJECT_TABLE;
			for (unsigned child = 0; child < 2; child++) {
				for (unsigned i = 0; i < lengthof(statements); i++) {
					int caught;

					reset_create_preflight();
					cluster_shared_catalog = (mode & 1) != 0;
					utility_storage_mode = (mode & 2) != 0;
					cluster_shared_config = (mode & 4) != 0;
					utility_in_child = child != 0;
					caught = sigsetjmp(boundary, 1);
					if (!caught)
						create_sql_entry(statements[i]);
					UT_ASSERT_EQ(caught != 0, (mode & 5) != 0);
					if (caught) {
						UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
						UT_ASSERT(strstr(error_message, "is not supported in shared mode") != NULL);
					}
					UT_ASSERT_EQ(utility_xids, 0);
					UT_ASSERT_EQ(utility_prepares, 0);
					UT_ASSERT_EQ(utility_binds, 0);
					UT_ASSERT_EQ(utility_after_preflight, (mode & 5) == 0);
				}
			}
		}
	}
}

typedef struct DropOwnedFixture {
	RoleSpec roles[2];
	ListCell cells[2];
	DropOwnedStmt statements[2];
	List role_list;
} DropOwnedFixture;

static void
init_drop_owned_fixture(DropOwnedFixture *fixture)
{
	MemSet(fixture, 0, sizeof(*fixture));
	fixture->roles[0]
		= (RoleSpec){ .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "owned_role" };
	fixture->roles[1] = (RoleSpec){ .type = T_RoleSpec, .roletype = ROLESPEC_CURRENT_USER };
	fixture->cells[0].ptr_value = &fixture->roles[0];
	fixture->cells[1].ptr_value = &fixture->roles[1];
	fixture->role_list
		= (List){ .type = T_List, .length = 2, .max_length = 2, .elements = fixture->cells };
	fixture->statements[0] = (DropOwnedStmt){ .type = T_DropOwnedStmt,
											  .roles = &fixture->role_list,
											  .behavior = DROP_RESTRICT };
	fixture->statements[1] = (DropOwnedStmt){ .type = T_DropOwnedStmt,
											  .roles = &fixture->role_list,
											  .behavior = DROP_CASCADE };
}

/* The original utility prologue is real; native role privileges, dependency
 * scans and deletion, like physical BIND durability, are later boundaries. */
UT_TEST(test_drop_owned_autocommit_binds_before_later_work_and_reuses_top)
{
	DropOwnedFixture fixture;

	init_drop_owned_fixture(&fixture);
	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned i = 0; i < lengthof(fixture.statements); i++) {
			bool target = (mode & 3) == 3;

			reset_create_preflight();
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			cluster_interconnect_tier = CLUSTER_IC_TIER_1;
			utility_in_transaction = drop_target_shared = false;
			storage_drop_preflights = 0;
			utility_expect_binding = target;
			create_sql_entry((Node *)&fixture.statements[i]);
			UT_ASSERT_EQ(utility_after_preflight, 1);
			UT_ASSERT_EQ(utility_unbound, 0);
			UT_ASSERT_EQ(utility_xids, target ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, target ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, target ? 1 : 0);
			create_sql_entry((Node *)&fixture.statements[i]);
			UT_ASSERT_EQ(utility_after_preflight, 2);
			UT_ASSERT_EQ(utility_unbound, 0);
			UT_ASSERT_EQ(utility_xids, target ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, target ? 2 : 0);
			UT_ASSERT_EQ(utility_binds, target ? 1 : 0);
			UT_ASSERT_EQ(storage_drop_preflights, 0);
		}
	}
}

UT_TEST(test_drop_owned_autocommit_refusal_child_and_wrong_top_precede_later_work)
{
	DropOwnedFixture fixture;

	init_drop_owned_fixture(&fixture);
	for (unsigned cause = 0; cause < 3; cause++) {
		for (unsigned i = 0; i < lengthof(fixture.statements); i++) {
			int caught;

			reset_create_preflight();
			cluster_interconnect_tier = CLUSTER_IC_TIER_1;
			utility_in_transaction = drop_target_shared = false;
			utility_bind_ok = cause != 0;
			utility_in_child = cause == 1;
			if (cause == 2) {
				utility_top_xid = 400;
				utility_current_xid = 401;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				create_sql_entry((Node *)&fixture.statements[i]);
			UT_ASSERT(caught);
			UT_ASSERT_EQ(error_code, cause == 0	  ? ERRCODE_DATA_CORRUPTED
									 : cause == 1 ? ERRCODE_FEATURE_NOT_SUPPORTED
												  : ERRCODE_INTERNAL_ERROR);
			if (cause != 2)
				UT_ASSERT(strstr(error_message, "shared catalog alteration") != NULL);
			UT_ASSERT_EQ(utility_xids, cause == 0 ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, cause == 0 ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, 0);
			UT_ASSERT_EQ(utility_after_preflight, 0);
		}
	}
}

UT_TEST(test_drop_owned_autocommit_readonly_parallel_precede_binding)
{
	DropOwnedFixture fixture;

	init_drop_owned_fixture(&fixture);
	for (unsigned parallel = 0; parallel < 2; parallel++) {
		for (unsigned i = 0; i < lengthof(fixture.statements); i++) {
			int caught;

			reset_create_preflight();
			cluster_interconnect_tier = CLUSTER_IC_TIER_1;
			utility_in_transaction = drop_target_shared = false;
			XactReadOnly = parallel == 0;
			utility_parallel = parallel != 0;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				create_sql_entry((Node *)&fixture.statements[i]);
			UT_ASSERT(caught);
			UT_ASSERT_EQ(error_code, parallel ? ERRCODE_INVALID_TRANSACTION_STATE
											  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_binds, 0);
			UT_ASSERT_EQ(utility_after_preflight, 0);
		}
	}
}

UT_TEST(test_drop_owned_locked_storage_precheck_keeps_original_path_without_binding)
{
	DropOwnedFixture fixture;

	init_drop_owned_fixture(&fixture);
	for (int tier = CLUSTER_IC_TIER_1; tier <= CLUSTER_IC_TIER_3; tier++) {
		for (unsigned transaction = 0; transaction < 2; transaction++) {
			if (tier == CLUSTER_IC_TIER_1 && transaction == 0)
				continue;
			for (unsigned mode = 0; mode < 8; mode++) {
				for (unsigned shared = 0; shared < 2; shared++) {
					for (unsigned i = 0; i < lengthof(fixture.statements); i++) {
						int caught;
						bool prechecked = (mode & 2) != 0;

						reset_create_preflight();
						cluster_shared_catalog = (mode & 1) != 0;
						utility_storage_mode = prechecked;
						cluster_shared_config = (mode & 4) != 0;
						cluster_interconnect_tier = tier;
						utility_in_transaction = transaction != 0;
						drop_target_shared = shared != 0;
						storage_drop_preflights = 0;
						caught = sigsetjmp(boundary, 1);
						if (!caught)
							create_sql_entry((Node *)&fixture.statements[i]);
						UT_ASSERT_EQ(caught != 0, prechecked && shared != 0);
						if (caught)
							UT_ASSERT_EQ(error_code, transaction ? ERRCODE_ACTIVE_SQL_TRANSACTION
																 : ERRCODE_FEATURE_NOT_SUPPORTED);
						UT_ASSERT_EQ(storage_drop_preflights, prechecked ? 1 : 0);
						UT_ASSERT_EQ(utility_xids, 0);
						UT_ASSERT_EQ(utility_prepares, 0);
						UT_ASSERT_EQ(utility_binds, 0);
						UT_ASSERT_EQ(utility_after_preflight, caught ? 0 : 1);
					}
				}
			}
		}
	}
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	utility_in_transaction = drop_target_shared = false;
}

typedef struct DropFixture {
	String names[2];
	ListCell name_cells[2];
	ListCell object_cell;
	DropStmt statement;
	List qualified_name;
} DropFixture;

static void
init_drop_fixture(DropFixture *fixture, List *objects, ObjectType kind)
{
	MemSet(fixture, 0, sizeof(*fixture));
	fixture->names[0] = (String){ .type = T_String, .sval = "preflight_schema" };
	fixture->names[1] = (String){ .type = T_String, .sval = "drop_target" };
	fixture->name_cells[0].ptr_value = &fixture->names[0];
	fixture->name_cells[1].ptr_value = &fixture->names[1];
	fixture->qualified_name
		= (List){ .type = T_List, .length = 2, .max_length = 2, .elements = fixture->name_cells };
	/* gram.y uses flat name_list for schema/extension, any_name_list otherwise. */
	fixture->object_cell.ptr_value = kind == OBJECT_SCHEMA || kind == OBJECT_EXTENSION
										 ? (void *)&fixture->names[1]
										 : (void *)&fixture->qualified_name;
	*objects
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &fixture->object_cell };
	fixture->statement = (DropStmt){ .type = T_DropStmt, .objects = objects, .removeType = kind };
}

static void
drop_sql_entry(DropStmt *statement, bool isTopLevel)
{
	PlannedStmt plan = { 0 };
	PlannedStmt *pstmt = &plan;
	Node *parsetree;
	const char *queryString = "";
	int readonly_flags;

	plan.utilityStmt = (Node *)statement;
#include "test_cluster_create_utility_entry.inc"
	if (utility_expect_binding && utility_binds == 0)
		utility_unbound++;
	utility_after_preflight++;
}

static const ObjectType drop_kinds[]
	= { OBJECT_TABLE,		  OBJECT_INDEX,	 OBJECT_SEQUENCE, OBJECT_VIEW,
		OBJECT_FOREIGN_TABLE, OBJECT_SCHEMA, OBJECT_COLLATION };

/* The native dependency closure/deletion and transaction-state classifier
 * remain boundaries; the original utility checks and preflight execute. */
UT_TEST(test_drop_autocommit_binds_before_later_work_and_reuses_top)
{
	DropFixture fixture;
	List objects;

	for (unsigned kind = 0; kind < lengthof(drop_kinds); kind++) {
		init_drop_fixture(&fixture, &objects, drop_kinds[kind]);
		for (unsigned mode = 0; mode < 8; mode++) {
			for (unsigned variant = 0; variant < 4; variant++) {
				bool target = (mode & 3) == 3;

				reset_create_preflight();
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				cluster_interconnect_tier = CLUSTER_IC_TIER_1;
				utility_in_transaction = drop_target_shared = false;
				storage_drop_preflights = 0;
				fixture.statement.behavior = variant & 1 ? DROP_CASCADE : DROP_RESTRICT;
				fixture.statement.missing_ok = (variant & 2) != 0;
				utility_expect_binding = target;
				drop_sql_entry(&fixture.statement, true);
				UT_ASSERT_EQ(utility_unbound, 0);
				UT_ASSERT_EQ(utility_after_preflight, 1);
				UT_ASSERT_EQ(utility_xids, target ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, target ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, target ? 1 : 0);
				drop_sql_entry(&fixture.statement, true);
				UT_ASSERT_EQ(utility_unbound, 0);
				UT_ASSERT_EQ(utility_after_preflight, 2);
				UT_ASSERT_EQ(utility_xids, target ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, target ? 2 : 0);
				UT_ASSERT_EQ(utility_binds, target ? 1 : 0);
				UT_ASSERT_EQ(storage_drop_preflights, 0);
			}
		}
	}
}

UT_TEST(test_drop_autocommit_refusal_and_wrong_top_precede_later_work)
{
	DropFixture fixture;
	List objects;

	for (unsigned kind = 0; kind < lengthof(drop_kinds); kind++) {
		init_drop_fixture(&fixture, &objects, drop_kinds[kind]);
		for (unsigned cause = 0; cause < 2; cause++) {
			int caught;

			reset_create_preflight();
			cluster_interconnect_tier = CLUSTER_IC_TIER_1;
			utility_in_transaction = drop_target_shared = false;
			utility_bind_ok = cause != 0;
			if (cause == 1) {
				utility_top_xid = 400;
				utility_current_xid = 401;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				drop_sql_entry(&fixture.statement, true);
			UT_ASSERT(caught);
			UT_ASSERT_EQ(error_code, cause == 0 ? ERRCODE_DATA_CORRUPTED : ERRCODE_INTERNAL_ERROR);
			if (cause == 0)
				UT_ASSERT(strstr(error_message, "shared catalog alteration") != NULL);
			UT_ASSERT_EQ(utility_xids, cause == 0 ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, cause == 0 ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, 0);
			UT_ASSERT_EQ(utility_after_preflight, 0);
		}
	}
}

UT_TEST(test_drop_autocommit_readonly_parallel_precede_binding)
{
	DropFixture fixture;
	List objects;

	for (unsigned kind = 0; kind < lengthof(drop_kinds); kind++) {
		init_drop_fixture(&fixture, &objects, drop_kinds[kind]);
		for (unsigned parallel = 0; parallel < 2; parallel++) {
			int caught;

			reset_create_preflight();
			cluster_interconnect_tier = CLUSTER_IC_TIER_1;
			utility_in_transaction = drop_target_shared = false;
			XactReadOnly = parallel == 0;
			utility_parallel = parallel != 0;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				drop_sql_entry(&fixture.statement, true);
			UT_ASSERT(caught);
			UT_ASSERT_EQ(error_code, parallel ? ERRCODE_INVALID_TRANSACTION_STATE
											  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_binds, 0);
			UT_ASSERT_EQ(utility_after_preflight, 0);
		}
	}
}

UT_TEST(test_drop_locked_or_concurrent_paths_keep_original_checks_without_binding)
{
	DropFixture fixture;
	List objects;

	for (unsigned kind = 0; kind < lengthof(drop_kinds); kind++) {
		init_drop_fixture(&fixture, &objects, drop_kinds[kind]);
		for (int tier = CLUSTER_IC_TIER_1; tier <= CLUSTER_IC_TIER_3; tier++) {
			for (unsigned scope = 0; scope < 3; scope++) {
				bool top = scope != 2;

				for (unsigned concurrent = 0; concurrent < 2; concurrent++) {
					if (concurrent && drop_kinds[kind] != OBJECT_INDEX)
						continue;
					if (tier == CLUSTER_IC_TIER_1 && scope == 0 && !concurrent)
						continue;
					for (unsigned mode = 0; mode < 8; mode++) {
						for (unsigned shared = 0; shared < 2; shared++) {
							bool prechecked
								= (mode & 2) != 0 && (tier != CLUSTER_IC_TIER_1 || scope != 0);
							bool index_rejected = concurrent && (mode & 5) != 0;
							int caught;

							reset_create_preflight();
							cluster_shared_catalog = (mode & 1) != 0;
							utility_storage_mode = (mode & 2) != 0;
							cluster_shared_config = (mode & 4) != 0;
							cluster_interconnect_tier = tier;
							utility_in_transaction = scope == 1;
							drop_target_shared = shared != 0;
							fixture.statement.concurrent = concurrent != 0;
							storage_drop_preflights = 0;
							caught = sigsetjmp(boundary, 1);
							if (!caught)
								drop_sql_entry(&fixture.statement, top);
							UT_ASSERT_EQ(caught != 0,
										 (prechecked && shared != 0) || index_rejected);
							if (caught)
								UT_ASSERT_EQ(error_code, prechecked && shared != 0 && scope != 0
															 ? ERRCODE_ACTIVE_SQL_TRANSACTION
															 : ERRCODE_FEATURE_NOT_SUPPORTED);
							if (index_rejected && !(prechecked && shared != 0))
								UT_ASSERT(strstr(error_message, "concurrent index removal")
										  != NULL);
							UT_ASSERT_EQ(storage_drop_preflights, prechecked ? 1 : 0);
							UT_ASSERT_EQ(utility_xids, 0);
							UT_ASSERT_EQ(utility_prepares, 0);
							UT_ASSERT_EQ(utility_binds, 0);
							UT_ASSERT_EQ(utility_after_preflight, caught ? 0 : 1);
						}
					}
				}
			}
		}
	}
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	utility_in_transaction = drop_target_shared = false;
}

UT_TEST(test_drop_unsupported_objects_refuse_before_binding)
{
	ObjectType kinds[] = { OBJECT_MATVIEW, OBJECT_EXTENSION };
	DropFixture fixture;
	List objects;

	for (unsigned kind = 0; kind < lengthof(kinds); kind++) {
		init_drop_fixture(&fixture, &objects, kinds[kind]);
		for (unsigned mode = 0; mode < 8; mode++) {
			int caught;

			reset_create_preflight();
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			cluster_interconnect_tier = CLUSTER_IC_TIER_1;
			utility_in_transaction = drop_target_shared = false;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				drop_sql_entry(&fixture.statement, true);
			UT_ASSERT_EQ(caught != 0, (mode & 5) != 0);
			if (caught) {
				UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
				UT_ASSERT(strstr(error_message, "is not supported in shared mode") != NULL);
			}
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_binds, 0);
			UT_ASSERT_EQ(utility_after_preflight, caught ? 0 : 1);
		}
	}
}

/* Execute the whole native import and its real filter. Catalog/ACL/FDW/parser
 * services and the post-prologue DDL executor are controlled boundaries; the
 * original utility prologue runs again for each returned child statement. */
static bool user_mapping_lookup_active, user_mapping_expect_binding;
static bool user_mapping_expected_missing_ok;
static unsigned user_mapping_lookups, user_mapping_unbound, user_mapping_bad_lookup;
static unsigned user_mapping_lookup_kind, user_mapping_expected_boundary;
static const RoleSpec *user_mapping_expected_role;
static ForeignServer import_server;
static ForeignDataWrapper import_fdw;
static FdwRoutine import_routine;
static unsigned import_lookups, import_callbacks, import_parses, import_children, import_ccis;
static unsigned import_unbound, import_bad_context;
static bool import_expect_binding;
static List *import_commands, *import_trees;
static ErrorContextCallback import_outer_context;
ErrorContextCallback *error_context_stack;
DestReceiver *None_Receiver;
MemoryContext CurrentMemoryContext;
static enum {
	IMPORT_OK,
	IMPORT_NO_SERVER,
	IMPORT_ACL_DENIED,
	IMPORT_NO_NAMESPACE,
	IMPORT_NO_HANDLER,
	IMPORT_NO_CALLBACK,
	IMPORT_CALLBACK_ERROR
} import_failure;

/* The diagnostic callback itself is outside this entry-order fixture. */
typedef struct {
	char *tablename;
	char *cmd;
} import_error_callback_arg;
static void
import_error_callback(void *arg)
{
	(void)arg;
}

int
errmsg_internal(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(error_message, sizeof(error_message), fmt, ap);
	va_end(ap);
	return 0;
}

void *
MemoryContextAllocZeroAligned(MemoryContext context, Size size)
{
	(void)context;
	return calloc(1, size);
}

char *
pstrdup(const char *str)
{
	return strdup(str);
}

static void
import_observe_binding(void)
{
	if (import_expect_binding && utility_binds == 0)
		import_unbound++;
}

ForeignServer *
GetForeignServerByName(const char *name, bool missing_ok)
{
	if (user_mapping_lookup_active) {
		user_mapping_lookups++;
		user_mapping_lookup_kind = 3;
		if (user_mapping_expect_binding && utility_binds == 0)
			user_mapping_unbound++;
		if (user_mapping_expected_boundary != 3 || !missing_ok
			|| strcmp(name, "mapping_server") != 0)
			user_mapping_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real server query. */
	}
	import_lookups++;
	import_observe_binding();
	if (missing_ok || strcmp(name, "server") != 0)
		import_bad_context++;
	if (import_failure == IMPORT_NO_SERVER)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("missing foreign server")));
	return &import_server;
}

Oid
GetUserId(void)
{
	return 10;
}

static bool publication_lookup_active, publication_expect_binding;
static unsigned publication_entry_kind, publication_lookups, publication_lookup_kind;
static unsigned publication_unbound, publication_bad_lookup;
Oid MyDatabaseId;

AclResult
object_aclcheck(Oid classid, Oid objectid, Oid roleid, AclMode mode)
{
	if (publication_lookup_active) {
		publication_lookups++;
		publication_lookup_kind = 1;
		if (publication_expect_binding && utility_binds == 0)
			publication_unbound++;
		if (publication_entry_kind != 0 || classid != DatabaseRelationId || objectid != 42
			|| roleid != 10 || mode != ACL_CREATE)
			publication_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real database ACL query. */
	}
	if (classid != ForeignServerRelationId || objectid != 20 || roleid != 10 || mode != ACL_USAGE)
		import_bad_context++;
	return import_failure == IMPORT_ACL_DENIED ? ACLCHECK_NO_PRIV : ACLCHECK_OK;
}

void
aclcheck_error(AclResult result, ObjectType type, const char *name)
{
	(void)result;
	(void)type;
	(void)name;
	ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("foreign server ACL denied")));
}

Oid
LookupCreationNamespace(const char *name)
{
	if (strcmp(name, "local") != 0)
		import_bad_context++;
	if (import_failure == IMPORT_NO_NAMESPACE)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("missing local schema")));
	return 30;
}

ForeignDataWrapper *
GetForeignDataWrapper(Oid fdwid)
{
	if (fdwid != 40)
		import_bad_context++;
	return &import_fdw;
}

FdwRoutine *
GetFdwRoutine(Oid handler)
{
	if (handler != 50)
		import_bad_context++;
	return &import_routine;
}

static List *
import_callback(ImportForeignSchemaStmt *stmt, Oid server)
{
	import_callbacks++;
	import_observe_binding();
	if (server != 20 || strcmp(stmt->remote_schema, "remote") != 0)
		import_bad_context++;
	if (import_failure == IMPORT_CALLBACK_ERROR)
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("foreign import callback failed")));
	return import_commands;
}

List *
pg_parse_query(const char *query)
{
	import_error_callback_arg *arg;

	import_parses++;
	import_observe_binding();
	if (error_context_stack == &import_outer_context || error_context_stack == NULL) {
		import_bad_context++;
		return import_trees;
	}
	arg = error_context_stack->arg;
	if (error_context_stack->previous != &import_outer_context
		|| error_context_stack->callback != import_error_callback || arg->cmd != query
		|| arg->tablename != NULL)
		import_bad_context++;
	return import_trees;
}

void
ProcessUtility(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
			   ProcessUtilityContext context, ParamListInfo params, QueryEnvironment *queryEnv,
			   DestReceiver *dest, QueryCompletion *qc)
{
	Node *parsetree;
	bool isTopLevel = context == PROCESS_UTILITY_TOPLEVEL;
	int readonly_flags;
	CreateForeignTableStmt *child = (CreateForeignTableStmt *)pstmt->utilityStmt;
	import_error_callback_arg *arg = error_context_stack->arg;

	import_children++;
	import_observe_binding();
	if (readOnlyTree || context != PROCESS_UTILITY_SUBCOMMAND || params != NULL || queryEnv != NULL
		|| dest != None_Receiver || qc != NULL || pstmt->commandType != CMD_UTILITY
		|| pstmt->canSetTag || strcmp(child->base.relation->schemaname, "local") != 0
		|| arg->tablename != child->base.relation->relname)
		import_bad_context++;
#include "test_cluster_create_utility_entry.inc"
	pfree(pstmt);
}

void
CommandCounterIncrement(void)
{
	import_ccis++;
}

#include "test_cluster_fresh_import_filter.inc"
#include "test_cluster_fresh_import.inc"

static ImportForeignSchemaStmt
reset_native_import(bool expect_binding)
{
	ImportForeignSchemaStmt stmt = { .type = T_ImportForeignSchemaStmt,
									 .server_name = "server",
									 .remote_schema = "remote",
									 .local_schema = "local",
									 .list_type = FDW_IMPORT_SCHEMA_ALL };

	reset_create_preflight();
	import_expect_binding = expect_binding;
	import_lookups = import_callbacks = import_parses = import_children = import_ccis = 0;
	import_unbound = import_bad_context = 0;
	import_failure = IMPORT_OK;
	import_commands = import_trees = NIL;
	import_server = (ForeignServer){ .serverid = 20, .fdwid = 40, .servername = "server" };
	import_fdw = (ForeignDataWrapper){ .fdwid = 40, .fdwname = "wrapper", .fdwhandler = 50 };
	import_routine.ImportForeignSchema = import_callback;
	error_context_stack = &import_outer_context;
	return stmt;
}

UT_TEST(test_native_import_binds_before_lookup_and_callback_in_all_profiles)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool binds = mode == 3 || mode == 7;
		ImportForeignSchemaStmt stmt = reset_native_import(binds);
		int caught;

		cluster_shared_catalog = (mode & 1) != 0;
		utility_storage_mode = (mode & 2) != 0;
		cluster_shared_config = (mode & 4) != 0;
		caught = sigsetjmp(boundary, 1);
		if (!caught) {
			ImportForeignSchema(&stmt);
			ImportForeignSchema(&stmt);
		}
		UT_ASSERT_EQ(caught, 0);
		UT_ASSERT_EQ(import_lookups, 2);
		UT_ASSERT_EQ(import_callbacks, 2);
		UT_ASSERT_EQ(import_unbound, 0);
		UT_ASSERT_EQ(import_bad_context, 0);
		UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
		UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
		UT_ASSERT_EQ(utility_prepares, binds ? 2 : 0);
		UT_ASSERT(error_context_stack == &import_outer_context);
	}
}

UT_TEST(test_native_import_refusal_and_invalid_scope_precede_catalogs)
{
	for (unsigned failure = 0; failure < 4; failure++) {
		ImportForeignSchemaStmt stmt = reset_native_import(true);
		int caught;

		if (failure == 0)
			utility_bind_ok = false;
		else if (failure == 1)
			utility_in_child = true;
		else {
			utility_current_xid = 401;
			utility_top_xid = failure == 2 ? 400 : InvalidTransactionId;
		}
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			ImportForeignSchema(&stmt);
		UT_ASSERT(caught != 0);
		UT_ASSERT_EQ(error_code, failure == 0	? ERRCODE_DATA_CORRUPTED
								 : failure == 1 ? ERRCODE_FEATURE_NOT_SUPPORTED
												: ERRCODE_INTERNAL_ERROR);
		UT_ASSERT_EQ(import_lookups, 0);
		UT_ASSERT_EQ(import_callbacks, 0);
		UT_ASSERT_EQ(utility_xids, failure == 0 ? 1 : 0);
		UT_ASSERT_EQ(utility_prepares, failure == 0 ? 1 : 0);
		UT_ASSERT_EQ(utility_binds, 0);
	}
}

UT_TEST(test_native_import_original_readonly_parallel_recovery_guards_precede_binding)
{
	for (unsigned guard = 0; guard < 3; guard++) {
		ImportForeignSchemaStmt stmt = reset_native_import(true);
		int caught;

		XactReadOnly = guard == 0;
		utility_parallel = guard == 1;
		utility_recovery = guard == 2;
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			ImportForeignSchema(&stmt);
		UT_ASSERT(caught != 0);
		UT_ASSERT_EQ(error_code, guard == 0	  ? ERRCODE_READ_ONLY_SQL_TRANSACTION
								 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
											  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
		UT_ASSERT_EQ(import_lookups, 0);
		UT_ASSERT_EQ(import_callbacks, 0);
		UT_ASSERT_EQ(utility_xids, 0);
		UT_ASSERT_EQ(utility_prepares, 0);
	}
	/* The nonshared direct native route retains its original behavior. */
	{
		ImportForeignSchemaStmt stmt = reset_native_import(false);

		cluster_shared_catalog = false;
		XactReadOnly = utility_parallel = utility_recovery = true;
		ImportForeignSchema(&stmt);
		UT_ASSERT_EQ(import_callbacks, 1);
		UT_ASSERT_EQ(utility_prepares, 0);
	}
}

UT_TEST(test_native_import_original_errors_and_empty_callback_keep_top_binding)
{
	const int errors[] = { 0,
						   ERRCODE_UNDEFINED_OBJECT,
						   ERRCODE_INSUFFICIENT_PRIVILEGE,
						   ERRCODE_UNDEFINED_SCHEMA,
						   ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
						   ERRCODE_FDW_NO_SCHEMAS,
						   ERRCODE_FDW_ERROR };

	for (unsigned failure = 0; failure < lengthof(errors); failure++) {
		ImportForeignSchemaStmt stmt = reset_native_import(true);
		int caught;

		import_failure = failure;
		if (failure == IMPORT_NO_HANDLER)
			import_fdw.fdwhandler = InvalidOid;
		if (failure == IMPORT_NO_CALLBACK)
			import_routine.ImportForeignSchema = NULL;
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			ImportForeignSchema(&stmt);
		UT_ASSERT_EQ(caught != 0, failure != IMPORT_OK);
		UT_ASSERT_EQ(error_code, errors[failure]);
		UT_ASSERT_EQ(import_lookups, 1);
		UT_ASSERT_EQ(import_callbacks,
					 failure == IMPORT_OK || failure == IMPORT_CALLBACK_ERROR ? 1 : 0);
		UT_ASSERT_EQ(import_parses, 0);
		UT_ASSERT_EQ(import_children, 0);
		UT_ASSERT_EQ(import_unbound, 0);
		UT_ASSERT_EQ(import_bad_context, 0);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_binds, 1);
	}
}

UT_TEST(test_native_import_filters_and_child_utility_reuse_original_top)
{
	for (unsigned filter = 0; filter < 4; filter++) {
		ImportForeignSchemaStmt stmt = reset_native_import(true);
		CreateForeignTableStmt children[3] = { 0 };
		RangeVar relations[3] = { 0 }, selected = { .type = T_RangeVar, .relname = "b" };
		RawStmt raw[3] = { 0 };
		TruncateStmt malformed = { .type = T_TruncateStmt };
		ListCell raw_cells[3], command_cell = { .ptr_value = "foreign commands" };
		ListCell selected_cell = { .ptr_value = &selected };
		List trees = { .type = T_List, .length = 3, .max_length = 3, .elements = raw_cells };
		List commands = { .type = T_List, .length = 1, .max_length = 1, .elements = &command_cell };
		List names = { .type = T_List, .length = 1, .max_length = 1, .elements = &selected_cell };
		const char *table_names[] = { "a", "b", "c" };
		unsigned expected = filter == 0 ? 3 : filter == 1 ? 1 : filter == 2 ? 2 : 0;
		int caught;

		for (unsigned i = 0; i < 3; i++) {
			relations[i].type = T_RangeVar;
			relations[i].relname = (char *)table_names[i];
			relations[i].schemaname = "remote";
			children[i].base.type = T_CreateForeignTableStmt;
			children[i].base.relation = &relations[i];
			raw[i].type = T_RawStmt;
			raw[i].stmt = (Node *)&children[i];
			raw_cells[i].ptr_value = &raw[i];
		}
		if (filter == 1)
			stmt.list_type = FDW_IMPORT_SCHEMA_LIMIT_TO;
		else if (filter == 2)
			stmt.list_type = FDW_IMPORT_SCHEMA_EXCEPT;
		if (filter == 3)
			raw[0].stmt = (Node *)&malformed;
		stmt.table_list = &names;
		import_commands = &commands;
		import_trees = &trees;
		caught = sigsetjmp(boundary, 1);
		if (!caught) {
			/* Ordinary utility has already published the same top binding. */
			create_sql_entry((Node *)&stmt);
			ImportForeignSchema(&stmt);
		}
		UT_ASSERT_EQ(caught != 0, filter == 3);
		if (caught)
			UT_ASSERT(strstr(error_message, "returned incorrect statement type") != NULL);
		else
			UT_ASSERT(error_context_stack == &import_outer_context);
		UT_ASSERT_EQ(import_callbacks, 1);
		UT_ASSERT_EQ(import_parses, 1);
		UT_ASSERT_EQ(import_children, expected);
		UT_ASSERT_EQ(import_ccis, expected);
		UT_ASSERT_EQ(import_unbound, 0);
		UT_ASSERT_EQ(import_bad_context, 0);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_binds, 1);
		UT_ASSERT_EQ(utility_prepares, 2 + expected);
		for (unsigned i = 0; i < 3; i++)
			if (strcmp(relations[i].schemaname, "local") == 0)
				pfree(relations[i].schemaname);
	}
}

/* These native prefixes include their original first catalog lookup. Stop
 * there with a distinct non-error jump; no lock, metadata or script executes.
 * The private control-file type is opaque because only a pointer is declared. */
typedef struct ExtensionControlFile ExtensionControlFile;
bool creating_extension;
static bool extension_expect_binding;
static unsigned extension_lookups, extension_unbound, extension_bad_lookup, extension_lookup_kind;
static String extension_name;
static AlterExtensionStmt extension_update;
static AlterExtensionContentsStmt extension_contents;
static bool tsearch_lookup_active, tsearch_expect_binding, tsearch_superuser_denied;
static unsigned tsearch_entry_kind, tsearch_expected_boundary, tsearch_lookup_kind;
static unsigned tsearch_lookups, tsearch_unbound, tsearch_bad_lookup;
static List tsearch_names;

static void
record_tsearch_lookup(unsigned kind)
{
	tsearch_lookups++;
	tsearch_lookup_kind = kind;
	if (tsearch_expect_binding && utility_binds == 0)
		tsearch_unbound++;
	if (kind != tsearch_expected_boundary)
		tsearch_bad_lookup++;
}

static bool schema_lookup_active, schema_expect_binding;
static unsigned schema_lookups, schema_unbound, schema_bad_lookup;
static bool comment_lookup_active, comment_expect_binding, comment_database_exists;
static unsigned comment_database_lookups, comment_object_lookups, comment_unbound,
	comment_bad_lookup;
static String comment_database_name, comment_table_name;
static ListCell comment_table_cell;
static List comment_table_names;
static CommentStmt native_comment_stmt;
static bool label_lookup_active, label_expect_binding;
static unsigned label_lookups, label_unbound, label_bad_lookup;
static SecLabelStmt native_label_stmt;
static bool collation_lookup_active, collation_expect_binding;
static unsigned collation_lookups, collation_unbound, collation_bad_lookup, collation_lookup_kind;
static String collation_name_node;
static ListCell collation_name_cell;
static List collation_names;
static bool foreign_server_lookup_active, foreign_server_expect_binding;
static unsigned foreign_server_lookups, foreign_server_unbound, foreign_server_bad_lookup;
static bool fdw_lookup_active, fdw_expect_binding;
static unsigned fdw_lookups, fdw_unbound, fdw_bad_lookup;
static bool function_lookup_active, function_expect_binding;
static unsigned function_entry_kind, function_lookups, function_unbound, function_bad_lookup;
static unsigned function_lookup_kind;
static String function_name_node;
static ListCell function_name_cell;
static List function_names;
static TypeName function_type, function_target_type;
static bool access_method_lookup_active, access_method_expect_binding;
static unsigned access_method_lookups, access_method_unbound, access_method_bad_lookup;
static bool event_trigger_lookup_active, event_trigger_expect_binding,
	event_trigger_superuser_denied;
static unsigned event_trigger_lookups, event_trigger_unbound, event_trigger_bad_lookup;
static unsigned event_trigger_lookup_kind, event_trigger_entry_kind;

/* Role settings boundaries stop before validation, database or role-cache work. */
static bool role_settings_lookup_active, role_settings_expect_binding;
static bool role_settings_superuser_denied;
static unsigned role_settings_lookups, role_settings_unbound, role_settings_bad_lookup;
static unsigned role_settings_expected_boundary, role_settings_lookup_kind;
static unsigned role_settings_unexpected_dependencies;
static RoleSpec role_settings_role;
static AlterRoleSetStmt native_role_settings_statement;
static const char *role_settings_expected_database;

static void
record_role_settings_lookup(unsigned kind)
{
	role_settings_lookups++;
	role_settings_lookup_kind = kind;
	if (role_settings_expect_binding && utility_binds == 0)
		role_settings_unbound++;
	if (kind != role_settings_expected_boundary)
		role_settings_bad_lookup++;
}

static bool database_settings_lookup_active, database_settings_expect_binding;
static unsigned database_settings_entry_kind, database_settings_lookups;
static unsigned database_settings_lookup_kind, database_settings_unbound;
static unsigned database_settings_bad_lookup;
static const char *database_settings_expected_name;

Oid
get_database_oid(const char *dbname, bool missing_ok)
{
	if (role_settings_lookup_active) {
		record_role_settings_lookup(2);
		if (native_role_settings_statement.role != NULL || dbname == NULL
			|| dbname != role_settings_expected_database || missing_ok)
			role_settings_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real database lookup. */
	}
	if (database_settings_lookup_active) {
		database_settings_lookups++;
		database_settings_lookup_kind = 1;
		if (database_settings_expect_binding && utility_binds == 0)
			database_settings_unbound++;
		if (database_settings_entry_kind != 0 || missing_ok
			|| strcmp(dbname, database_settings_expected_name) != 0)
			database_settings_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real database query. */
	}
	comment_database_lookups++;
	if (comment_expect_binding && utility_binds == 0)
		comment_unbound++;
	if (!comment_lookup_active || strcmp(dbname, "comment_database") != 0 || !missing_ok
		|| (comment_database_exists && comment_object_lookups + 1 != comment_database_lookups))
		comment_bad_lookup++;
	return comment_database_exists ? 500 : InvalidOid;
}

/* Native role rename stops before its first catalog lock. */
static bool role_rename_lookup_active, role_rename_expect_binding;
static unsigned role_rename_lookups, role_rename_unbound, role_rename_bad_lookup;

Relation
table_open(Oid relationId, LOCKMODE lockmode)
{
	if (database_identity_lookup_active) {
		database_identity_lookups++;
		if (database_identity_expect_binding && utility_binds == 0)
			database_identity_unbound++;
		if (relationId != DatabaseRelationId || lockmode != RowExclusiveLock)
			database_identity_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real database catalog lock/query. */
	}
	if (role_rename_lookup_active) {
		role_rename_lookups++;
		if (role_rename_expect_binding && utility_binds == 0)
			role_rename_unbound++;
		if (relationId != AuthIdRelationId || lockmode != RowExclusiveLock)
			role_rename_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real role catalog lock/query. */
	}
	if (database_settings_lookup_active) {
		database_settings_lookups++;
		database_settings_lookup_kind = 2;
		if (database_settings_expect_binding && utility_binds == 0)
			database_settings_unbound++;
		if (database_settings_entry_kind != 1 || relationId != DatabaseRelationId
			|| lockmode != RowExclusiveLock)
			database_settings_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real database catalog lock. */
	}
	if (publication_lookup_active) {
		publication_lookups++;
		publication_lookup_kind = 2;
		if (publication_expect_binding && utility_binds == 0)
			publication_unbound++;
		if (publication_entry_kind != 1 || relationId != PublicationRelationId
			|| lockmode != RowExclusiveLock)
			publication_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real publication catalog lock. */
	}
	if (user_mapping_lookup_active) {
		user_mapping_lookups++;
		user_mapping_lookup_kind = 1;
		if (user_mapping_expect_binding && utility_binds == 0)
			user_mapping_unbound++;
		if (user_mapping_expected_boundary != 1 || relationId != UserMappingRelationId
			|| lockmode != RowExclusiveLock)
			user_mapping_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real mapping catalog lock. */
	}
	if (foreign_server_lookup_active) {
		foreign_server_lookups++;
		if (foreign_server_expect_binding && utility_binds == 0)
			foreign_server_unbound++;
		if (relationId != ForeignServerRelationId || lockmode != RowExclusiveLock)
			foreign_server_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real server catalog lock. */
	}
	if (fdw_lookup_active) {
		fdw_lookups++;
		if (fdw_expect_binding && utility_binds == 0)
			fdw_unbound++;
		if (relationId != ForeignDataWrapperRelationId || lockmode != RowExclusiveLock)
			fdw_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real wrapper catalog lock. */
	}
	if (function_lookup_active) {
		function_lookups++;
		function_lookup_kind = 2;
		if (function_expect_binding && utility_binds == 0)
			function_unbound++;
		if (function_entry_kind < 2 || function_entry_kind > 4 || relationId != ProcedureRelationId
			|| lockmode != RowExclusiveLock)
			function_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real pg_proc lock. */
	}
	if (access_method_lookup_active) {
		access_method_lookups++;
		if (access_method_expect_binding && utility_binds == 0)
			access_method_unbound++;
		if (relationId != AccessMethodRelationId || lockmode != RowExclusiveLock)
			access_method_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real catalog lock. */
	}
	if (event_trigger_lookup_active) {
		event_trigger_lookups++;
		event_trigger_lookup_kind = 2;
		if (event_trigger_expect_binding && utility_binds == 0)
			event_trigger_unbound++;
		if (event_trigger_entry_kind == 0 || relationId != EventTriggerRelationId
			|| lockmode != RowExclusiveLock)
			event_trigger_bad_lookup++;
		siglongjmp(boundary, 2);
	}
	if (collation_lookup_active) {
		collation_lookups++;
		collation_lookup_kind = 2;
		if (collation_expect_binding && utility_binds == 0)
			collation_unbound++;
		if (relationId != CollationRelationId || lockmode != RowExclusiveLock)
			collation_bad_lookup++;
		siglongjmp(boundary, 2);
	}
	if (schema_lookup_active) {
		schema_lookups++;
		if (schema_expect_binding && utility_binds == 0)
			schema_unbound++;
		if (relationId != NamespaceRelationId || lockmode != RowExclusiveLock)
			schema_bad_lookup++;
		siglongjmp(boundary, 2);
	}
	extension_lookups++;
	extension_lookup_kind = 1;
	if (extension_expect_binding && utility_binds == 0)
		extension_unbound++;
	if (relationId != ExtensionRelationId || lockmode != AccessShareLock)
		extension_bad_lookup++;
	siglongjmp(boundary, 2);
}

String *
makeString(char *str)
{
	extension_name.type = T_String;
	extension_name.sval = str;
	return &extension_name;
}

ObjectAddress
get_object_address(ObjectType objtype, Node *object, Relation *relp, LOCKMODE lockmode,
				   bool missing_ok)
{
	if (schema_dispatch_lookup_active) {
		schema_dispatch_lookups++;
		schema_dispatch_lookup_kind = 4;
		if (schema_dispatch_expect_binding && utility_binds == 0)
			schema_dispatch_unbound++;
		if (schema_dispatch_expected_boundary != 4
			|| objtype != native_schema_dispatch_statement.objectType
			|| object != native_schema_dispatch_statement.object || relp == NULL
			|| lockmode != AccessExclusiveLock || missing_ok)
			schema_dispatch_bad_lookup++;
		siglongjmp(boundary, 2); /* Before actual object resolution or lock. */
	}
	if (rename_dispatch_lookup_active) {
		rename_dispatch_lookups++;
		rename_dispatch_lookup_kind = 12;
		if (rename_dispatch_expect_binding && utility_binds == 0)
			rename_dispatch_unbound++;
		if (rename_dispatch_expected_boundary != 12
			|| objtype != native_rename_dispatch_statement.renameType
			|| object != native_rename_dispatch_statement.object || relp == NULL
			|| lockmode != AccessExclusiveLock || missing_ok)
			rename_dispatch_bad_lookup++;
		siglongjmp(boundary, 2); /* Before actual object resolution or lock. */
	}
	if (label_lookup_active) {
		label_lookups++;
		if (label_expect_binding && utility_binds == 0)
			label_unbound++;
		if (objtype != native_label_stmt.objtype || object != native_label_stmt.object
			|| relp == NULL || lockmode != ShareUpdateExclusiveLock || missing_ok)
			label_bad_lookup++;
		siglongjmp(boundary, 2);
	}
	if (comment_lookup_active) {
		comment_object_lookups++;
		if (comment_expect_binding && utility_binds == 0)
			comment_unbound++;
		if (objtype != native_comment_stmt.objtype || object != native_comment_stmt.object
			|| relp == NULL || lockmode != ShareUpdateExclusiveLock || missing_ok
			|| (objtype == OBJECT_DATABASE && comment_database_lookups != comment_object_lookups))
			comment_bad_lookup++;
		siglongjmp(boundary, 2);
	}
	extension_lookups++;
	extension_lookup_kind = 2;
	if (extension_expect_binding && utility_binds == 0)
		extension_unbound++;
	if (objtype != OBJECT_EXTENSION || !IsA(object, String)
		|| strcmp(strVal(object), "extension") != 0 || relp == NULL || lockmode != AccessShareLock
		|| missing_ok)
		extension_bad_lookup++;
	siglongjmp(boundary, 2);
}

/* Full CREATE runs validation/options; its internal install is a boundary.
 * Namespace stops at the original first catalog lookup, before real locks. */
static bool scope_extension_exists;
static unsigned scope_extension_oid_calls, scope_extension_inner_calls,
	scope_extension_bad_boundary;
static const char *scope_extension_schema, *scope_extension_version;
static bool scope_extension_cascade;

Oid
get_extension_oid(const char *name, bool missing_ok)
{
	scope_extension_oid_calls++;
	if (strcmp(name, "extension") != 0)
		scope_extension_bad_boundary++;
	if (!missing_ok)
		siglongjmp(boundary, 2);
	return scope_extension_exists ? 60 : InvalidOid;
}

static ObjectAddress
CreateExtensionInternal(char *name, char *schema, const char *version, bool cascade, List *parents,
						bool is_create)
{
	scope_extension_inner_calls++;
	scope_extension_schema = schema;
	scope_extension_version = version;
	scope_extension_cascade = cascade;
	if (strcmp(name, "extension") != 0 || parents != NIL || !is_create)
		scope_extension_bad_boundary++;
	siglongjmp(boundary, 2);
}

#include "test_cluster_fresh_extension.inc"

static void
reset_native_extension(bool expect_binding)
{
	reset_create_preflight();
	creating_extension = false;
	extension_expect_binding = expect_binding;
	extension_lookups = extension_unbound = extension_bad_lookup = extension_lookup_kind = 0;
	extension_update = (AlterExtensionStmt){ .type = T_AlterExtensionStmt, .extname = "extension" };
	extension_contents = (AlterExtensionContentsStmt){ .type = T_AlterExtensionContentsStmt,
													   .extname = "extension",
													   .objtype = OBJECT_TABLE };
}

static Node *
native_extension_statement(unsigned kind)
{
	if (kind == 0)
		return (Node *)&extension_update;
	extension_contents.action = kind == 1 ? 1 : -1;
	return (Node *)&extension_contents;
}

static void
native_extension_entry(unsigned kind)
{
	Node *stmt = native_extension_statement(kind);

	if (kind == 0)
		(void)ExecAlterExtensionStmt(NULL, (AlterExtensionStmt *)stmt);
	else
		(void)ExecAlterExtensionContentsStmt((AlterExtensionContentsStmt *)stmt, NULL);
}

UT_TEST(test_native_extension_binds_before_catalog_lock_in_all_profiles)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_extension(binds);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_extension_entry(kind);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(extension_lookups, repeat + 1);
				UT_ASSERT_EQ(extension_lookup_kind, kind == 0 ? 1 : 2);
				UT_ASSERT_EQ(extension_unbound, 0);
				UT_ASSERT_EQ(extension_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
			}
		}
	}
}

UT_TEST(test_native_extension_refusal_child_and_wrong_top_precede_catalog_lock)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned failure = 0; failure < 4; failure++) {
			int caught;

			reset_native_extension(true);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1)
				utility_in_child = true;
			else {
				utility_current_xid = 401;
				utility_top_xid = failure == 2 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_extension_entry(kind);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0	? ERRCODE_DATA_CORRUPTED
									 : failure == 1 ? ERRCODE_FEATURE_NOT_SUPPORTED
													: ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_EQ(extension_lookups, 0);
			UT_ASSERT_EQ(utility_xids, failure == 0 ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, failure == 0 ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, 0);
		}
	}
}

UT_TEST(test_native_extension_original_readonly_parallel_recovery_guards_precede_binding)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned guard = 0; guard < 3; guard++) {
			int caught;

			reset_native_extension(true);
			XactReadOnly = guard == 0;
			utility_parallel = guard == 1;
			utility_recovery = guard == 2;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_extension_entry(kind);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
												: ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(extension_lookups, 0);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
		}
		/* No new context restrictions on the original nonshared native route. */
		{
			int caught;

			reset_native_extension(false);
			cluster_shared_catalog = false;
			XactReadOnly = utility_parallel = utility_recovery = true;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_extension_entry(kind);
			UT_ASSERT_EQ(caught, 2);
			UT_ASSERT_EQ(extension_lookups, 1);
			UT_ASSERT_EQ(utility_prepares, 0);
		}
	}
}

UT_TEST(test_native_extension_nested_and_forbidden_objects_refuse_before_binding)
{
	ObjectType forbidden[]
		= { OBJECT_DATABASE, OBJECT_EXTENSION,	   OBJECT_INDEX,		OBJECT_PUBLICATION,
			OBJECT_ROLE,	 OBJECT_STATISTIC_EXT, OBJECT_SUBSCRIPTION, OBJECT_TABLESPACE };

	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			for (unsigned object = 0; object < (kind == 0 ? 1 : lengthof(forbidden)); object++) {
				int caught;

				reset_native_extension(false);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				creating_extension = kind == 0;
				extension_contents.objtype = forbidden[object];
				XactReadOnly = utility_parallel = utility_recovery = utility_in_child = true;
				utility_bind_ok = false;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_extension_entry(kind);
				UT_ASSERT_EQ(caught, 1);
				UT_ASSERT_EQ(error_code, kind == 0 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INVALID_OBJECT_DEFINITION);
				UT_ASSERT(strstr(error_message, kind == 0 ? "nested ALTER EXTENSION"
														  : "cannot add an object of this type")
						  != NULL);
				UT_ASSERT_EQ(extension_lookups, 0);
				UT_ASSERT_EQ(utility_xids, 0);
				UT_ASSERT_EQ(utility_prepares, 0);
			}
		}
	}
}

UT_TEST(test_native_extension_utility_prebinding_reuses_same_top_before_lookup)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		int caught;

		reset_native_extension(true);
		create_sql_entry(native_extension_statement(kind));
		UT_ASSERT_EQ(utility_prepares, 1);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_binds, 1);
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			native_extension_entry(kind);
		UT_ASSERT_EQ(caught, 2);
		UT_ASSERT_EQ(extension_lookups, 1);
		UT_ASSERT_EQ(extension_unbound, 0);
		UT_ASSERT_EQ(extension_bad_lookup, 0);
		UT_ASSERT_EQ(utility_prepares, 2);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_binds, 1);
	}
}

static void
reset_native_extension_scope(void)
{
	reset_native_extension(false);
	scope_extension_exists = false;
	scope_extension_oid_calls = scope_extension_inner_calls = scope_extension_bad_boundary = 0;
	scope_extension_schema = scope_extension_version = NULL;
	scope_extension_cascade = false;
}

static void
unsupported_extension_entry(bool create, const char *name, bool if_not_exists, Oid *oldschema)
{
	if (create) {
		CreateExtensionStmt stmt = { .type = T_CreateExtensionStmt,
									 .extname = (char *)name,
									 .if_not_exists = if_not_exists };

		(void)CreateExtension(NULL, &stmt);
	} else
		(void)AlterExtensionNamespace(name, "target", oldschema);
}

UT_TEST(test_native_unsupported_extension_commands_refuse_in_original_shared_profiles)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			for (unsigned skip = 0; skip < 2; skip++) {
				bool shared = mode != 0 && mode != 2;
				Oid oldschema = 91;
				int caught;

				reset_native_extension_scope();
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				scope_extension_exists = skip != 0;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					unsupported_extension_entry(kind == 0, "extension", skip != 0, &oldschema);
				UT_ASSERT_EQ(caught, shared ? 1 : kind == 0 && skip ? 0 : 2);
				if (shared) {
					UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
					UT_ASSERT_STR_EQ(error_message,
									 kind == 0
										 ? "CREATE EXTENSION is not supported in shared mode"
										 : "CREATE/DROP EXTENSION is not supported in shared mode");
					UT_ASSERT_STR_EQ(
						error_detail,
						"PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
				}
				UT_ASSERT_EQ(scope_extension_oid_calls, shared ? 0 : 1);
				UT_ASSERT_EQ(scope_extension_inner_calls, !shared && kind == 0 && !skip ? 1 : 0);
				UT_ASSERT_EQ(scope_extension_bad_boundary, 0);
				UT_ASSERT_EQ(oldschema, 91);
				UT_ASSERT_EQ(utility_xids, 0);
				UT_ASSERT_EQ(utility_binds, 0);
				UT_ASSERT_EQ(utility_prepares, 0);
			}
		}
	}
}

UT_TEST(test_native_unsupported_extension_refusal_precedes_invalid_context_and_allocation)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned context = 0; context < 7; context++) {
			Oid oldschema = 91;
			int caught;

			reset_native_extension_scope();
			cluster_shared_catalog = (context & 1) != 0;
			cluster_shared_config = !cluster_shared_catalog;
			utility_storage_mode = false;
			XactReadOnly = context == 0;
			utility_parallel = context == 1;
			utility_recovery = context == 2;
			utility_in_child = context == 3;
			utility_bind_ok = false;
			if (context == 4) {
				utility_current_xid = 401;
				utility_top_xid = 400;
			}
			if (context == 5) {
				utility_current_xid = utility_top_xid = 400;
				utility_binds = 1;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				unsupported_extension_entry(kind == 0, context == 6 ? "" : "extension", false,
											&oldschema);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
			UT_ASSERT_STR_EQ(error_detail,
							 "PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
			UT_ASSERT_EQ(scope_extension_oid_calls, 0);
			UT_ASSERT_EQ(scope_extension_inner_calls, 0);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_binds, context == 5 ? 1 : 0);
			UT_ASSERT_EQ(oldschema, 91);
		}
	}
}

UT_TEST(test_nonshared_extension_create_retains_native_validation_skip_nested_and_options)
{
	const char *invalid[] = { "", "-x", "x-", "x--y", "x/y" };

	for (unsigned i = 0; i < lengthof(invalid); i++) {
		int caught;

		reset_native_extension_scope();
		cluster_shared_catalog = cluster_shared_config = false;
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			unsupported_extension_entry(true, invalid[i], false, NULL);
		UT_ASSERT_EQ(caught, 1);
		UT_ASSERT_EQ(error_code, ERRCODE_INVALID_PARAMETER_VALUE);
		UT_ASSERT_EQ(scope_extension_oid_calls, 0);
		UT_ASSERT_EQ(scope_extension_inner_calls, 0);
		UT_ASSERT_EQ(utility_prepares, 0);
	}
	for (unsigned variant = 0; variant < 5; variant++) {
		CreateExtensionStmt stmt = { .type = T_CreateExtensionStmt, .extname = "extension" };
		String schema = { .type = T_String, .sval = "chosen" };
		String version = { .type = T_String, .sval = "v2" };
		DefElem options[]
			= { { .type = T_DefElem, .defname = "schema", .arg = (Node *)&schema },
				{ .type = T_DefElem, .defname = "new_version", .arg = (Node *)&version },
				{ .type = T_DefElem, .defname = "cascade" } };
		ListCell cells[3];
		List list = { .type = T_List, .length = 3, .max_length = 3, .elements = cells };
		ObjectAddress result = { 0 };
		int caught;

		reset_native_extension_scope();
		cluster_shared_catalog = cluster_shared_config = false;
		for (unsigned i = 0; i < 3; i++)
			cells[i].ptr_value = &options[i];
		if (variant <= 1) {
			scope_extension_exists = true;
			stmt.if_not_exists = variant == 1;
		} else if (variant == 2)
			creating_extension = true;
		else {
			stmt.options = &list;
			if (variant == 4) {
				list.length = 2;
				cells[1].ptr_value = &options[0];
			}
		}
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			result = CreateExtension(NULL, &stmt);
		UT_ASSERT_EQ(caught, variant == 1 ? 0 : variant == 3 ? 2 : 1);
		if (variant == 0)
			UT_ASSERT_EQ(error_code, ERRCODE_DUPLICATE_OBJECT);
		else if (variant == 1)
			UT_ASSERT_EQ(result.objectId, InvalidOid);
		else if (variant == 2)
			UT_ASSERT(strstr(error_message, "nested CREATE EXTENSION") != NULL);
		else if (variant == 4)
			UT_ASSERT_EQ(error_code, ERRCODE_SYNTAX_ERROR);
		if (variant == 3) {
			UT_ASSERT_STR_EQ(scope_extension_schema, "chosen");
			UT_ASSERT_STR_EQ(scope_extension_version, "v2");
			UT_ASSERT(scope_extension_cascade);
		}
		UT_ASSERT_EQ(scope_extension_oid_calls, 1);
		UT_ASSERT_EQ(scope_extension_inner_calls, variant == 3 ? 1 : 0);
		UT_ASSERT_EQ(scope_extension_bad_boundary, 0);
		UT_ASSERT_EQ(utility_prepares, 0);
	}
}

UT_TEST(test_utility_and_native_unsupported_extension_code_message_detail_match)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			CreateExtensionStmt create = { .type = T_CreateExtensionStmt, .extname = "extension" };
			String name = { .type = T_String, .sval = "extension" };
			AlterObjectSchemaStmt move = { .type = T_AlterObjectSchemaStmt,
										   .objectType = OBJECT_EXTENSION,
										   .object = (Node *)&name,
										   .newschema = "target" };
			bool shared = mode != 0 && mode != 2;
			char message[256], detail[160];
			int caught;

			reset_native_extension_scope();
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				create_sql_entry(kind == 0 ? (Node *)&create : (Node *)&move);
			UT_ASSERT_EQ(caught, shared ? 1 : 0);
			strlcpy(message, error_message, sizeof(message));
			strlcpy(detail, error_detail, sizeof(detail));
			reset_error();
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				unsupported_extension_entry(kind == 0, "extension", false, NULL);
			UT_ASSERT_EQ(caught, shared ? 1 : 2);
			if (shared) {
				UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
				UT_ASSERT_STR_EQ(error_message, message);
				UT_ASSERT_STR_EQ(error_detail, detail);
			}
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_binds, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
		}
	}
}

/* These actual native prefixes stop at their first table_open before a real
 * lock. The helper is complete; later namespace ACL/hook work is not run. */
#include "test_cluster_fresh_schema.inc"

static void
reset_native_schema(bool expect_binding)
{
	reset_create_preflight();
	schema_lookup_active = true;
	schema_expect_binding = expect_binding;
	schema_lookups = schema_unbound = schema_bad_lookup = 0;
}

static void
native_schema_entry(unsigned kind)
{
	if (kind == 0)
		(void)RenameSchema("original", "renamed");
	else
		(void)AlterSchemaOwner("original", 700);
}

UT_TEST(test_native_schema_changes_bind_before_catalog_lock_in_all_profiles)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_schema(binds);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_schema_entry(kind);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(schema_lookups, repeat + 1);
				UT_ASSERT_EQ(schema_unbound, 0);
				UT_ASSERT_EQ(schema_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	schema_lookup_active = false;
}

UT_TEST(test_native_schema_refusal_child_or_wrong_top_precedes_catalog_lock)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned config = 0; config < 2; config++) {
			for (unsigned failure = 0; failure < 5; failure++) {
				int caught;

				reset_native_schema(true);
				cluster_shared_config = config != 0;
				if (failure == 0)
					utility_bind_ok = false;
				else if (failure == 1 || failure == 4) {
					if (failure == 4)
						create_sql_entry((Node *)&(AlterOwnerStmt){ .type = T_AlterOwnerStmt,
																	.objectType = OBJECT_SCHEMA });
					utility_in_child = true;
				} else {
					utility_current_xid = 401;
					utility_top_xid = failure == 2 ? 400 : InvalidTransactionId;
				}
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_schema_entry(kind);
				UT_ASSERT_EQ(caught, 1);
				UT_ASSERT_EQ(error_code, failure == 0 ? ERRCODE_DATA_CORRUPTED
										 : failure == 1 || failure == 4
											 ? ERRCODE_FEATURE_NOT_SUPPORTED
											 : ERRCODE_INTERNAL_ERROR);
				UT_ASSERT_EQ(schema_lookups, 0);
				UT_ASSERT_EQ(utility_xids, failure == 0 || failure == 4 ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, failure == 0 || failure == 4 ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, failure == 4 ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, failure == 0 || failure == 2 || failure == 4
												  ? 400
												  : InvalidTransactionId);
			}
		}
	}
	schema_lookup_active = false;
}

UT_TEST(test_native_schema_context_guards_precede_binding_and_preserve_nonshared_route)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_schema(binds);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_schema_entry(kind);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(schema_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(schema_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, 0);
				UT_ASSERT_EQ(utility_prepares, 0);
				UT_ASSERT_EQ(utility_binds, 0);
			}
		}
	}
	schema_lookup_active = false;
}

UT_TEST(test_native_schema_utility_prebinding_reuses_same_top_before_lookup)
{
	RenameStmt rename = { .type = T_RenameStmt, .renameType = OBJECT_SCHEMA };
	AlterOwnerStmt owner = { .type = T_AlterOwnerStmt, .objectType = OBJECT_SCHEMA };
	Node *statements[] = { (Node *)&rename, (Node *)&owner };

	for (unsigned kind = 0; kind < 2; kind++) {
		int caught;

		reset_native_schema(true);
		create_sql_entry(statements[kind]);
		UT_ASSERT_EQ(utility_prepares, 1);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_binds, 1);
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			native_schema_entry(kind);
		UT_ASSERT_EQ(caught, 2);
		UT_ASSERT_EQ(error_code, 0);
		UT_ASSERT_EQ(schema_lookups, 1);
		UT_ASSERT_EQ(schema_unbound, 0);
		UT_ASSERT_EQ(schema_bad_lookup, 0);
		UT_ASSERT_EQ(utility_prepares, 2);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_binds, 1);
		UT_ASSERT_EQ(utility_top_xid, 400);
	}
	schema_lookup_active = false;
}

/* Run the actual COMMENT prologue through its first object-lock lookup.
 * Missing databases take the real earlier return, without any catalog write. */
#include "test_cluster_fresh_comment.inc"

static void
reset_native_comment(bool expect_binding, bool database)
{
	reset_create_preflight();
	comment_lookup_active = true;
	comment_expect_binding = expect_binding;
	comment_database_exists = true;
	comment_database_lookups = comment_object_lookups = comment_unbound = comment_bad_lookup = 0;
	comment_database_name = (String){ .type = T_String, .sval = "comment_database" };
	comment_table_name = (String){ .type = T_String, .sval = "comment_table" };
	comment_table_cell = (ListCell){ .ptr_value = &comment_table_name };
	comment_table_names
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &comment_table_cell };
	native_comment_stmt = (CommentStmt){ .type = T_CommentStmt,
										 .objtype = database ? OBJECT_DATABASE : OBJECT_TABLE,
										 .object = database ? (Node *)&comment_database_name
															: (Node *)&comment_table_names,
										 .comment = "comment body" };
}

UT_TEST(test_native_comment_binds_before_database_and_object_lookup_in_all_profiles)
{
	for (unsigned database = 0; database < 2; database++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_comment(binds, database != 0);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)CommentObject(&native_comment_stmt);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(comment_database_lookups, database ? repeat + 1 : 0);
				UT_ASSERT_EQ(comment_object_lookups, repeat + 1);
				UT_ASSERT_EQ(comment_unbound, 0);
				UT_ASSERT_EQ(comment_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	comment_lookup_active = false;
}

UT_TEST(test_native_comment_refusal_child_and_wrong_top_precede_all_lookups)
{
	for (unsigned database = 0; database < 2; database++) {
		for (unsigned config = 0; config < 2; config++) {
			for (unsigned failure = 0; failure < 5; failure++) {
				bool allocated = failure == 0 || failure == 2;
				int caught;

				reset_native_comment(true, database != 0);
				cluster_shared_config = config != 0;
				if (failure == 0)
					utility_bind_ok = false;
				else if (failure == 1 || failure == 2) {
					if (failure == 2)
						create_sql_entry((Node *)&native_comment_stmt);
					utility_in_child = true;
				} else {
					utility_current_xid = 401;
					utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
				}
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)CommentObject(&native_comment_stmt);
				UT_ASSERT_EQ(caught, 1);
				UT_ASSERT_EQ(error_code, failure == 0 ? ERRCODE_DATA_CORRUPTED
										 : failure == 1 || failure == 2
											 ? ERRCODE_FEATURE_NOT_SUPPORTED
											 : ERRCODE_INTERNAL_ERROR);
				UT_ASSERT_EQ(comment_database_lookups, 0);
				UT_ASSERT_EQ(comment_object_lookups, 0);
				UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid,
							 allocated || failure == 3 ? 400 : InvalidTransactionId);
			}
		}
	}
	comment_lookup_active = false;
}

UT_TEST(test_native_comment_context_guards_keep_nonshared_native_route)
{
	for (unsigned database = 0; database < 2; database++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_comment(binds, database != 0);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)CommentObject(&native_comment_stmt);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(comment_database_lookups, binds ? 0 : database);
				UT_ASSERT_EQ(comment_object_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(comment_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, 0);
				UT_ASSERT_EQ(utility_prepares, 0);
				UT_ASSERT_EQ(utility_binds, 0);
			}
		}
	}
	comment_lookup_active = false;
}

UT_TEST(test_native_comment_utility_warm_binding_and_context_rejection_preserve_top)
{
	for (unsigned database = 0; database < 2; database++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_comment(true, database != 0);
			create_sql_entry((Node *)&native_comment_stmt);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)CommentObject(&native_comment_stmt);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(comment_database_lookups, guard ? 0 : database);
			UT_ASSERT_EQ(comment_object_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(comment_unbound, 0);
			UT_ASSERT_EQ(comment_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	comment_lookup_active = false;
}

UT_TEST(test_native_comment_missing_database_retains_early_return_and_binding_cost)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool binds = mode == 3 || mode == 7;

		reset_native_comment(binds, true);
		cluster_shared_catalog = (mode & 1) != 0;
		utility_storage_mode = (mode & 2) != 0;
		cluster_shared_config = (mode & 4) != 0;
		comment_database_exists = false;
		for (unsigned repeat = 0; repeat < 2; repeat++) {
			ObjectAddress address = { 1, 2, 3 };
			int caught = sigsetjmp(boundary, 1);

			if (!caught)
				address = CommentObject(&native_comment_stmt);
			UT_ASSERT_EQ(caught, 0);
			UT_ASSERT_EQ(error_code, 0);
			UT_ASSERT_EQ(address.classId, InvalidObjectAddress.classId);
			UT_ASSERT_EQ(address.objectId, InvalidObjectAddress.objectId);
			UT_ASSERT_EQ(address.objectSubId, InvalidObjectAddress.objectSubId);
			UT_ASSERT_EQ(comment_database_lookups, repeat + 1);
			UT_ASSERT_EQ(comment_object_lookups, 0);
			UT_ASSERT_EQ(comment_unbound, 0);
			UT_ASSERT_EQ(comment_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
			UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
		}
	}
	comment_lookup_active = false;
}

/* Actual provider representation, type validation and native prefix run here.
 * Provider hooks and object locks never execute past the jump2 boundary. */
#include "test_cluster_fresh_seclabel.inc"
static LabelProvider label_providers[2];
static ListCell label_provider_cells[2], label_object_cell;
static List label_providers_list, label_object_names;
static String label_object_name;

static void
reset_native_label(bool expect_binding, bool named)
{
	reset_create_preflight();
	label_lookup_active = true;
	label_expect_binding = expect_binding;
	label_lookups = label_unbound = label_bad_lookup = 0;
	label_providers[0] = (LabelProvider){ .provider_name = "label_one" };
	label_providers[1] = (LabelProvider){ .provider_name = "label_two" };
	label_provider_cells[0] = (ListCell){ .ptr_value = &label_providers[0] };
	label_provider_cells[1] = (ListCell){ .ptr_value = &label_providers[1] };
	label_providers_list = (List){ .type = T_List,
								   .length = named ? 2 : 1,
								   .max_length = 2,
								   .elements = label_provider_cells };
	label_provider_list = &label_providers_list;
	label_object_name = (String){ .type = T_String, .sval = "label_table" };
	label_object_cell = (ListCell){ .ptr_value = &label_object_name };
	label_object_names
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &label_object_cell };
	native_label_stmt = (SecLabelStmt){ .type = T_SecLabelStmt,
										.objtype = OBJECT_TABLE,
										.object = (Node *)&label_object_names,
										.provider = named ? "label_two" : NULL,
										.label = "label value" };
}

UT_TEST(test_native_security_labels_bind_before_object_lock_in_all_profiles)
{
	for (unsigned named = 0; named < 2; named++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_label(binds, named != 0);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)ExecSecLabelStmt(&native_label_stmt);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(label_lookups, repeat + 1);
				UT_ASSERT_EQ(label_unbound, 0);
				UT_ASSERT_EQ(label_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	label_lookup_active = false;
}

UT_TEST(test_native_security_label_refusals_precede_object_lock)
{
	for (unsigned named = 0; named < 2; named++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_label(true, named != 0);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry((Node *)&native_label_stmt);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)ExecSecLabelStmt(&native_label_stmt);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0					? ERRCODE_DATA_CORRUPTED
									 : failure == 1 || failure == 2 ? ERRCODE_FEATURE_NOT_SUPPORTED
																	: ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_EQ(label_lookups, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	label_lookup_active = false;
}

UT_TEST(test_native_security_label_context_guards_keep_nonshared_route)
{
	for (unsigned named = 0; named < 2; named++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_label(binds, named != 0);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)ExecSecLabelStmt(&native_label_stmt);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(label_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(label_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, 0);
				UT_ASSERT_EQ(utility_prepares, 0);
				UT_ASSERT_EQ(utility_binds, 0);
			}
		}
	}
	label_lookup_active = false;
}

UT_TEST(test_native_security_label_original_provider_and_type_errors_precede_allocation)
{
	const char *messages[] = { "no security label providers", "must specify provider",
							   "is not loaded", "not supported for this type of object" };

	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned failure = 0; failure < 4; failure++) {
			int caught;

			reset_native_label(false, failure == 1 || failure == 2);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			if (failure == 0)
				label_provider_list = NIL;
			else if (failure == 1)
				native_label_stmt.provider = NULL;
			else if (failure == 2)
				native_label_stmt.provider = "missing";
			else
				native_label_stmt.objtype = OBJECT_INDEX;
			XactReadOnly = utility_parallel = utility_recovery = utility_in_child = true;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)ExecSecLabelStmt(&native_label_stmt);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 3 ? ERRCODE_WRONG_OBJECT_TYPE
												  : ERRCODE_INVALID_PARAMETER_VALUE);
			UT_ASSERT(strstr(error_message, messages[failure]) != NULL);
			UT_ASSERT_EQ(label_lookups, 0);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_binds, 0);
		}
	}
	label_lookup_active = false;
}

UT_TEST(test_native_security_label_utility_warm_context_guards_preserve_binding)
{
	for (unsigned named = 0; named < 2; named++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_label(true, named != 0);
			create_sql_entry((Node *)&native_label_stmt);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)ExecSecLabelStmt(&native_label_stmt);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(label_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(label_unbound, 0);
			UT_ASSERT_EQ(label_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	label_lookup_active = false;
}

UT_TEST(test_native_security_label_original_shared_scope_matches_utility_refusal)
{
	ObjectType forbidden[] = { OBJECT_MATVIEW, OBJECT_TYPE,		OBJECT_DOMAIN,	 OBJECT_OPERATOR,
							   OBJECT_OPCLASS, OBJECT_OPFAMILY, OBJECT_EXTENSION };

	for (unsigned mode = 0; mode < 8; mode++) {
		if (mode == 0 || mode == 2)
			continue;
		for (unsigned type = 0; type < lengthof(forbidden); type++) {
			char message[sizeof(error_message)], detail[sizeof(error_detail)];
			int caught, code;

			reset_native_label(false, false);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			native_label_stmt.objtype = forbidden[type];
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				create_sql_entry((Node *)&native_label_stmt);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
			code = error_code;
			memcpy(message, error_message, sizeof(message));
			memcpy(detail, error_detail, sizeof(detail));
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			reset_error();
			label_provider_list = NIL;
			XactReadOnly = utility_parallel = utility_recovery = utility_in_child = true;
			utility_bind_ok = false;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)ExecSecLabelStmt(&native_label_stmt);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, code);
			UT_ASSERT_STR_EQ(error_message, message);
			UT_ASSERT_STR_EQ(error_detail, detail);
			UT_ASSERT_EQ(label_lookups, 0);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_binds, 0);
		}
	}
	label_lookup_active = false;
}

UT_TEST(test_native_security_label_nonshared_object_type_routes_unchanged)
{
	ObjectType types[] = { OBJECT_MATVIEW, OBJECT_TYPE,		OBJECT_DOMAIN,	 OBJECT_OPERATOR,
						   OBJECT_OPCLASS, OBJECT_OPFAMILY, OBJECT_EXTENSION };

	for (unsigned storage = 0; storage < 2; storage++) {
		for (unsigned type = 0; type < lengthof(types); type++) {
			int caught;

			reset_native_label(false, false);
			cluster_shared_catalog = cluster_shared_config = false;
			utility_storage_mode = storage != 0;
			native_label_stmt.objtype = types[type];
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)ExecSecLabelStmt(&native_label_stmt);
			UT_ASSERT_EQ(caught, type < 3 ? 2 : 1);
			UT_ASSERT_EQ(error_code, type < 3 ? 0 : ERRCODE_WRONG_OBJECT_TYPE);
			UT_ASSERT_EQ(label_lookups, type < 3 ? 1 : 0);
			UT_ASSERT_EQ(label_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, 0);
			UT_ASSERT_EQ(utility_prepares, 0);
			UT_ASSERT_EQ(utility_binds, 0);
		}
	}
	label_lookup_active = false;
}

/* Run original policy syntax/role helpers and native prefixes. Array/parser
 * setup is controlled; the relation lookup jumps2 before any real lock. */
static bool policy_expect_binding;
static unsigned policy_lookups, policy_role_lookups, policy_arrays, policy_parses;
static unsigned policy_unbound, policy_bad_boundary;
static bool policy_explicit_role;
static RangeVar policy_target;
static RoleSpec policy_role;
static ListCell policy_role_cell;
static List policy_roles;
static CreatePolicyStmt native_policy_create;
static AlterPolicyStmt native_policy_alter;
static RenameStmt native_policy_rename;
static Node *policy_expected_statement;
static ParseState policy_parse_states[2];
static ArrayType policy_role_array;

static void
RangeVarCallbackForPolicy(const RangeVar *rv, Oid relid, Oid oldrelid, void *arg)
{
	(void)rv;
	(void)relid;
	(void)oldrelid;
	(void)arg;
	abort(); /* This callback must remain beyond the real lookup boundary. */
}

/* Role membership writes stop before their first grantor or grantee lookup. */
static bool role_grant_lookup_active, role_grant_expect_binding;
static unsigned role_grant_lookups, role_grant_lookup_kind, role_grant_expected_boundary;
static unsigned role_grant_unbound, role_grant_bad_lookup, role_grant_unexpected_dependencies;
static RoleSpec role_grant_grantor;
static List role_grant_grantees;

static bool owner_lookup_active, owner_expect_binding;
static unsigned owner_lookups, owner_unbound, owner_bad_lookup;
static RoleSpec owner_role;

/* Object privilege changes stop before native grantor or target resolution. */
static bool acl_grant_lookup_active, acl_grant_expect_binding, acl_grant_mismatched_grantor;
static unsigned acl_grant_lookups, acl_grant_lookup_kind, acl_grant_expected_boundary;
static unsigned acl_grant_unbound, acl_grant_bad_lookup;
static GrantStmt native_acl_grant_statement;
static RoleSpec acl_grant_grantor;
static List acl_grant_objects;

/* Default privileges stop before their first role or default-ACL query. */
static bool default_acl_lookup_active, default_acl_expect_binding;
static unsigned default_acl_lookups, default_acl_lookup_kind, default_acl_expected_boundary;
static unsigned default_acl_unbound, default_acl_bad_lookup, default_acl_local_appends;
static unsigned default_acl_unexpected_dependencies;
static RoleSpec default_acl_grantee, default_acl_owner;
static List default_acl_public_oids;
static ListCell default_acl_public_oid_cell;

Oid
get_rolespec_oid(const RoleSpec *role, bool missing_ok)
{
	if (default_acl_lookup_active) {
		default_acl_lookups++;
		default_acl_lookup_kind = default_acl_expected_boundary == 1 ? 1 : 2;
		if (default_acl_expect_binding && utility_binds == 0)
			default_acl_unbound++;
		if (default_acl_expected_boundary == 3 || missing_ok
			|| role
				   != (default_acl_expected_boundary == 1 ? &default_acl_grantee
														  : &default_acl_owner)
			|| role->roletype != ROLESPEC_CSTRING
			|| default_acl_local_appends != (default_acl_expected_boundary == 2 ? 1U : 0U))
			default_acl_bad_lookup++;
		siglongjmp(boundary, 2); /* Before the actual grantee or owner query. */
	}
	if (acl_grant_lookup_active) {
		acl_grant_lookups++;
		acl_grant_lookup_kind = 1;
		if (acl_grant_expect_binding && utility_binds == 0)
			acl_grant_unbound++;
		if (acl_grant_expected_boundary != 1 || role != &acl_grant_grantor || missing_ok)
			acl_grant_bad_lookup++;
		if (acl_grant_mismatched_grantor)
			return 11;			 /* Controlled mismatch exercises the original refusal. */
		siglongjmp(boundary, 2); /* Before the actual grantor query. */
	}
	if (role_grant_lookup_active) {
		role_grant_lookups++;
		role_grant_lookup_kind = 1;
		if (role_grant_expect_binding && utility_binds == 0)
			role_grant_unbound++;
		if (role_grant_expected_boundary != 1 || role != &role_grant_grantor || missing_ok)
			role_grant_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the actual grantor query. */
	}
	if (user_mapping_lookup_active) {
		user_mapping_lookups++;
		user_mapping_lookup_kind = 2;
		if (user_mapping_expect_binding && utility_binds == 0)
			user_mapping_unbound++;
		if (user_mapping_expected_boundary != 2 || role != user_mapping_expected_role
			|| role->roletype == ROLESPEC_PUBLIC || missing_ok != user_mapping_expected_missing_ok)
			user_mapping_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real role query. */
	}
	if (owner_lookup_active) {
		owner_lookups++;
		if (owner_expect_binding && utility_binds == 0)
			owner_unbound++;
		if (role != &owner_role || role->roletype != ROLESPEC_CSTRING
			|| strcmp(role->rolename, "owner_role") != 0 || missing_ok)
			owner_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real role lookup. */
	}
	policy_role_lookups++;
	if (policy_expect_binding && utility_binds == 0)
		policy_unbound++;
	if (role != &policy_role || role->roletype != ROLESPEC_CSTRING
		|| strcmp(role->rolename, "policy_role") != 0 || missing_ok)
		policy_bad_boundary++;
	return 700;
}

ArrayType *
construct_array_builtin(Datum *elems, int nelems, Oid elmtype)
{
	policy_arrays++;
	if (policy_expect_binding && utility_binds == 0)
		policy_unbound++;
	if (nelems != 1 || elmtype != OIDOID || elems == NULL
		|| DatumGetObjectId(elems[0]) != (policy_explicit_role ? 700 : ACL_ID_PUBLIC))
		policy_bad_boundary++;
	return &policy_role_array;
}

ParseState *
make_parsestate(ParseState *parent)
{
	policy_parses++;
	if (policy_expect_binding && utility_binds == 0)
		policy_unbound++;
	if (parent != NULL)
		policy_bad_boundary++;
	return &policy_parse_states[(policy_parses - 1) % 2];
}

/* Sequence alteration stops at its first target lookup, before locking or callbacks. */
static bool sequence_alter_lookup_active, sequence_alter_expect_binding;
static unsigned sequence_alter_lookups, sequence_alter_unbound, sequence_alter_bad_lookup;
static unsigned sequence_alter_unexpected_callbacks;
static uint32 sequence_alter_expected_flags;
static AlterSeqStmt native_sequence_alter_statement;

void
RangeVarCallbackOwnsRelation(const RangeVar *relation, Oid relid, Oid oldrelid, void *arg)
{
	(void)relation;
	(void)relid;
	(void)oldrelid;
	(void)arg;
	sequence_alter_unexpected_callbacks++;
	siglongjmp(boundary, 3); /* This lookup boundary must not invoke the owner callback. */
}

Oid
RangeVarGetRelidExtended(const RangeVar *relation, LOCKMODE lockmode, uint32 flags,
						 RangeVarGetRelidCallback callback, void *callback_arg)
{
	if (sequence_alter_lookup_active) {
		sequence_alter_lookups++;
		if (sequence_alter_expect_binding && utility_binds == 0)
			sequence_alter_unbound++;
		if (relation != native_sequence_alter_statement.sequence
			|| lockmode != ShareRowExclusiveLock || flags != sequence_alter_expected_flags
			|| callback != RangeVarCallbackOwnsRelation || callback_arg != NULL)
			sequence_alter_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real lookup, lock or owner callback. */
	}
	policy_lookups++;
	if (policy_expect_binding && utility_binds == 0)
		policy_unbound++;
	if (relation != &policy_target || lockmode != AccessExclusiveLock || flags != 0
		|| callback != RangeVarCallbackForPolicy || callback_arg != policy_expected_statement)
		policy_bad_boundary++;
	siglongjmp(boundary, 2);
}

#include "test_cluster_fresh_policy.inc"

static void
reset_native_policy(bool expect_binding, unsigned kind)
{
	reset_create_preflight();
	policy_expect_binding = expect_binding;
	policy_lookups = policy_role_lookups = policy_arrays = policy_parses = 0;
	policy_unbound = policy_bad_boundary = 0;
	policy_explicit_role = kind == 1 || kind == 3;
	policy_target = (RangeVar){ .type = T_RangeVar, .relname = "policy_table" };
	policy_role
		= (RoleSpec){ .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "policy_role" };
	policy_role_cell = (ListCell){ .ptr_value = &policy_role };
	policy_roles
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &policy_role_cell };
	native_policy_create
		= (CreatePolicyStmt){ .type = T_CreatePolicyStmt,
							  .policy_name = "policy",
							  .table = &policy_target,
							  .cmd_name = "all",
							  .permissive = true,
							  .roles = policy_explicit_role ? &policy_roles : NIL };
	native_policy_alter = (AlterPolicyStmt){ .type = T_AlterPolicyStmt,
											 .policy_name = "policy",
											 .table = &policy_target,
											 .roles = policy_explicit_role ? &policy_roles : NIL };
	native_policy_rename = (RenameStmt){ .type = T_RenameStmt,
										 .renameType = OBJECT_POLICY,
										 .relation = &policy_target,
										 .subname = "policy",
										 .newname = "renamed" };
	policy_expected_statement = kind < 2   ? (Node *)&native_policy_create
								: kind < 4 ? (Node *)&native_policy_alter
										   : (Node *)&native_policy_rename;
}

static void
native_policy_entry(unsigned kind)
{
	if (kind < 2)
		(void)CreatePolicy(&native_policy_create);
	else if (kind < 4)
		(void)AlterPolicy(&native_policy_alter);
	else
		(void)rename_policy(&native_policy_rename);
}

UT_TEST(test_native_policy_changes_bind_before_role_setup_and_table_lock_in_all_profiles)
{
	for (unsigned kind = 0; kind < 5; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_policy(binds, kind);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_policy_entry(kind);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(policy_lookups, repeat + 1);
				UT_ASSERT_EQ(policy_role_lookups, policy_explicit_role ? repeat + 1 : 0);
				UT_ASSERT_EQ(policy_arrays, kind < 2 || kind == 3 ? repeat + 1 : 0);
				UT_ASSERT_EQ(policy_parses, kind < 2 ? 2 * (repeat + 1) : 0);
				UT_ASSERT_EQ(policy_unbound, 0);
				UT_ASSERT_EQ(policy_bad_boundary, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
			}
		}
	}
}

UT_TEST(test_native_policy_refusals_precede_role_setup_and_table_lock)
{
	for (unsigned kind = 0; kind < 5; kind++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_policy(true, kind);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(policy_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_policy_entry(kind);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0					? ERRCODE_DATA_CORRUPTED
									 : failure == 1 || failure == 2 ? ERRCODE_FEATURE_NOT_SUPPORTED
																	: ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_EQ(policy_lookups + policy_role_lookups + policy_arrays + policy_parses, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
			if (failure == 1 || failure == 2)
				UT_ASSERT_STR_EQ(
					error_message,
					kind < 2 ? "shared catalog creation does not support subtransactions"
							 : "shared catalog alteration does not support subtransactions");
		}
	}
}

UT_TEST(test_native_policy_context_guards_keep_nonshared_route)
{
	for (unsigned kind = 0; kind < 5; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_policy(binds, kind);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_policy_entry(kind);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(policy_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(policy_bad_boundary, 0);
				UT_ASSERT_EQ(utility_xids, 0);
				UT_ASSERT_EQ(utility_prepares, 0);
				if (binds)
					UT_ASSERT_EQ(policy_role_lookups + policy_arrays + policy_parses, 0);
			}
		}
	}
}

UT_TEST(test_native_policy_utility_warm_context_guards_preserve_binding)
{
	for (unsigned kind = 0; kind < 5; kind++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_policy(true, kind);
			create_sql_entry(policy_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_policy_entry(kind);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(policy_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(policy_unbound, 0);
			UT_ASSERT_EQ(policy_bad_boundary, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
}

UT_TEST(test_native_policy_create_retains_syntax_errors_before_allocation_and_valid_commands)
{
	const char *commands[] = { "all", "select", "insert", "update", "delete" };
	const char *invalid_commands[] = { NULL, "unknown", "select", "delete", "insert" };
	Node expression = { .type = T_A_Const };

	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned invalid = 0; invalid < lengthof(invalid_commands); invalid++) {
			int caught;

			reset_native_policy(false, 0);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			native_policy_create.cmd_name = (char *)invalid_commands[invalid];
			if (invalid == 2 || invalid == 3)
				native_policy_create.with_check = &expression;
			if (invalid == 4)
				native_policy_create.qual = &expression;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)CreatePolicy(&native_policy_create);
			UT_ASSERT_EQ(caught, 1);
			if (invalid < 2)
				UT_ASSERT_STR_EQ(error_message, "unrecognized policy command");
			else {
				UT_ASSERT_EQ(error_code, ERRCODE_SYNTAX_ERROR);
				UT_ASSERT_STR_EQ(error_message,
								 invalid < 4 ? "WITH CHECK cannot be applied to SELECT or DELETE"
											 : "only WITH CHECK expression allowed for INSERT");
			}
			UT_ASSERT_EQ(policy_lookups + policy_role_lookups + policy_arrays + policy_parses, 0);
			UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
		}
	}
	for (unsigned command = 0; command < lengthof(commands); command++) {
		int caught;

		reset_native_policy(true, command % 2);
		native_policy_create.cmd_name = (char *)commands[command];
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			(void)CreatePolicy(&native_policy_create);
		UT_ASSERT_EQ(caught, 2);
		UT_ASSERT_EQ(error_code, 0);
		UT_ASSERT_EQ(policy_lookups, 1);
		UT_ASSERT_EQ(policy_unbound, 0);
		UT_ASSERT_EQ(policy_bad_boundary, 0);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_prepares, 1);
		UT_ASSERT_EQ(utility_binds, 1);
	}
}

/* The actual conversion prefix stops before namespace catalog lookup. The
 * conversion function and catalog write remain beyond this boundary. */
static bool conversion_expect_binding;
static unsigned conversion_lookups, conversion_unbound, conversion_bad_lookup;
static String conversion_name_node;
static ListCell conversion_name_cell;
static List conversion_names;
static CreateConversionStmt native_conversion_stmt;

static bool opclass_lookup_active;
static unsigned opclass_entry_kind, opclass_lookups, opclass_bad_lookup, opclass_lookup_kind;
static List opclass_names;
static char *opclass_amname;

static bool aggregate_lookup_active, aggregate_expect_binding;
static unsigned aggregate_lookups, aggregate_unbound, aggregate_bad_lookup;
static List aggregate_names;

static bool operator_lookup_active;
static unsigned operator_entry_kind, operator_lookups, operator_bad_lookup, operator_lookup_kind;
static List operator_names;
static ObjectWithArgs native_operator_args;

Oid
QualifiedNameGetCreationNamespace(List *names, char **objname)
{
	if (tsearch_lookup_active) {
		record_tsearch_lookup(2);
		if ((tsearch_entry_kind != 1 && tsearch_entry_kind != 3) || names != &tsearch_names
			|| objname == NULL)
			tsearch_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real text-search namespace query. */
	}
	if (aggregate_lookup_active) {
		aggregate_lookups++;
		if (aggregate_expect_binding && utility_binds == 0)
			aggregate_unbound++;
		if (names != &aggregate_names || objname == NULL)
			aggregate_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real aggregate namespace query. */
	}
	if (opclass_lookup_active) {
		opclass_lookups++;
		opclass_lookup_kind = 1;
		if (opclass_entry_kind >= 4 || names != &opclass_names || objname == NULL)
			opclass_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real class/family namespace lookup. */
	}
	if (operator_lookup_active) {
		operator_lookups++;
		operator_lookup_kind = 1;
		if (operator_entry_kind >= 2 || names != &operator_names || objname == NULL)
			operator_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real operator namespace lookup. */
	}
	if (function_lookup_active) {
		function_lookups++;
		function_lookup_kind = 1;
		if (function_expect_binding && utility_binds == 0)
			function_unbound++;
		if (function_entry_kind > 1 || names != &function_names || objname == NULL)
			function_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real namespace query. */
	}
	if (collation_lookup_active) {
		collation_lookups++;
		collation_lookup_kind = 1;
		if (collation_expect_binding && utility_binds == 0)
			collation_unbound++;
		if (names != &collation_names || objname == NULL)
			collation_bad_lookup++;
		siglongjmp(boundary, 2);
	}
	conversion_lookups++;
	if (conversion_expect_binding && utility_binds == 0)
		conversion_unbound++;
	if (names != &conversion_names || objname == NULL)
		conversion_bad_lookup++;
	siglongjmp(boundary, 2);
}

#include "test_cluster_fresh_conversion.inc"

static void
reset_native_conversion(bool expect_binding)
{
	reset_create_preflight();
	conversion_expect_binding = expect_binding;
	conversion_lookups = conversion_unbound = conversion_bad_lookup = 0;
	conversion_name_node = (String){ .type = T_String, .sval = "conversion" };
	conversion_name_cell = (ListCell){ .ptr_value = &conversion_name_node };
	conversion_names
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &conversion_name_cell };
	native_conversion_stmt = (CreateConversionStmt){ .type = T_CreateConversionStmt,
													 .conversion_name = &conversion_names,
													 .for_encoding_name = "UTF8",
													 .to_encoding_name = "LATIN1",
													 .func_name = &conversion_names };
}

UT_TEST(test_native_conversion_binds_before_namespace_lookup_in_all_profiles)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool binds = mode == 3 || mode == 7;
		int caught;

		reset_native_conversion(binds);
		cluster_shared_catalog = (mode & 1) != 0;
		utility_storage_mode = (mode & 2) != 0;
		cluster_shared_config = (mode & 4) != 0;
		for (unsigned repeat = 0; repeat < 2; repeat++) {
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)CreateConversionCommand(&native_conversion_stmt);
			UT_ASSERT_EQ(caught, 2);
			UT_ASSERT_EQ(error_code, 0);
			UT_ASSERT_EQ(conversion_lookups, repeat + 1);
			UT_ASSERT_EQ(conversion_unbound, 0);
			UT_ASSERT_EQ(conversion_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
		}
	}
}

UT_TEST(test_native_conversion_refusals_precede_namespace_lookup)
{
	for (unsigned failure = 0; failure < 5; failure++) {
		bool allocated = failure == 0 || failure == 2;
		int caught;

		reset_native_conversion(true);
		if (failure == 0)
			utility_bind_ok = false;
		else if (failure == 1 || failure == 2) {
			if (failure == 2)
				create_sql_entry((Node *)&native_conversion_stmt);
			utility_in_child = true;
		} else {
			utility_current_xid = 401;
			utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
		}
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			(void)CreateConversionCommand(&native_conversion_stmt);
		UT_ASSERT_EQ(caught, 1);
		UT_ASSERT_EQ(error_code, failure == 0					? ERRCODE_DATA_CORRUPTED
								 : failure == 1 || failure == 2 ? ERRCODE_FEATURE_NOT_SUPPORTED
																: ERRCODE_INTERNAL_ERROR);
		UT_ASSERT_STR_EQ(error_message,
						 failure == 0
							 ? "canonical ACTIVE publication failed before shared catalog creation"
						 : failure == 1 || failure == 2
							 ? "shared catalog creation does not support subtransactions"
							 : "shared catalog top-level transaction identity mismatch");
		UT_ASSERT_EQ(conversion_lookups, 0);
		UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
		UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
		UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
		UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
	}
}

UT_TEST(test_native_conversion_context_guards_keep_nonshared_route)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool binds = mode == 3 || mode == 7;

		for (unsigned guard = 0; guard < 3; guard++) {
			int caught;

			reset_native_conversion(binds);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			XactReadOnly = guard == 0;
			utility_parallel = guard == 1;
			utility_recovery = guard == 2;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)CreateConversionCommand(&native_conversion_stmt);
			UT_ASSERT_EQ(caught, binds ? 1 : 2);
			UT_ASSERT_EQ(error_code, !binds		  ? 0
									 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(conversion_lookups, binds ? 0 : 1);
			UT_ASSERT_EQ(conversion_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
		}
	}
}

UT_TEST(test_native_conversion_utility_warm_context_guards_preserve_binding)
{
	for (unsigned guard = 0; guard < 4; guard++) {
		int caught;

		reset_native_conversion(true);
		create_sql_entry((Node *)&native_conversion_stmt);
		UT_ASSERT_EQ(utility_prepares, 1);
		UT_ASSERT_EQ(utility_binds, 1);
		XactReadOnly = guard == 1;
		utility_parallel = guard == 2;
		utility_recovery = guard == 3;
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			(void)CreateConversionCommand(&native_conversion_stmt);
		UT_ASSERT_EQ(caught, guard ? 1 : 2);
		UT_ASSERT_EQ(error_code, !guard		  ? 0
								 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
											  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
		UT_ASSERT_EQ(conversion_lookups, guard ? 0 : 1);
		UT_ASSERT_EQ(conversion_unbound, 0);
		UT_ASSERT_EQ(conversion_bad_lookup, 0);
		UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_binds, 1);
		UT_ASSERT_EQ(utility_top_xid, 400);
	}
}

/* Actual native collation prefixes stop before the first namespace lookup or
 * pg_collation lock. Provider/version work and catalog writes do not execute. */
#include "test_cluster_fresh_collation.inc"
static DefineStmt native_collation_create;
static AlterCollationStmt native_collation_alter;

static void
reset_native_collation(bool expect_binding)
{
	reset_create_preflight();
	collation_lookup_active = true;
	collation_expect_binding = expect_binding;
	collation_lookups = collation_unbound = collation_bad_lookup = collation_lookup_kind = 0;
	collation_name_node = (String){ .type = T_String, .sval = "collation" };
	collation_name_cell = (ListCell){ .ptr_value = &collation_name_node };
	collation_names
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &collation_name_cell };
	native_collation_create = (DefineStmt){ .type = T_DefineStmt,
											.kind = OBJECT_COLLATION,
											.defnames = &collation_names };
	native_collation_alter
		= (AlterCollationStmt){ .type = T_AlterCollationStmt, .collname = &collation_names };
}

static void
native_collation_entry(unsigned kind)
{
	if (kind == 0)
		(void)DefineCollation(NULL, &collation_names, NIL, false);
	else
		(void)AlterCollation(&native_collation_alter);
}

UT_TEST(test_native_collations_bind_before_namespace_lookup_or_catalog_lock_in_all_profiles)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_collation(binds);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_collation_entry(kind);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(collation_lookups, repeat + 1);
				UT_ASSERT_EQ(collation_lookup_kind, kind == 0 ? 1 : 2);
				UT_ASSERT_EQ(collation_unbound, 0);
				UT_ASSERT_EQ(collation_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	collation_lookup_active = false;
}

UT_TEST(test_native_collation_refusals_precede_lookup_or_lock)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_collation(true);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(kind == 0 ? (Node *)&native_collation_create
											   : (Node *)&native_collation_alter);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_collation_entry(kind);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0					? ERRCODE_DATA_CORRUPTED
									 : failure == 1 || failure == 2 ? ERRCODE_FEATURE_NOT_SUPPORTED
																	: ERRCODE_INTERNAL_ERROR);
			if (failure == 1 || failure == 2)
				UT_ASSERT_STR_EQ(
					error_message,
					kind == 0 ? "shared catalog creation does not support subtransactions"
							  : "shared catalog alteration does not support subtransactions");
			UT_ASSERT_EQ(collation_lookups, 0);
			UT_ASSERT_EQ(collation_lookup_kind, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	collation_lookup_active = false;
}

UT_TEST(test_native_collation_context_guards_keep_nonshared_route)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_collation(binds);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_collation_entry(kind);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(collation_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(collation_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	collation_lookup_active = false;
}

UT_TEST(test_native_collation_utility_warm_context_guards_preserve_binding)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_collation(true);
			create_sql_entry(kind == 0 ? (Node *)&native_collation_create
									   : (Node *)&native_collation_alter);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_collation_entry(kind);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(collation_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(collation_unbound, 0);
			UT_ASSERT_EQ(collation_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	collation_lookup_active = false;
}

/* Actual CREATE includes its original permission refusal. Normally superuser
 * jumps before role-cache lookup; false exercises that real refusal. */
static bool language_lookup_active, language_expect_binding, language_superuser_denied;
static unsigned language_lookups, language_unbound;

bool
superuser(void)
{
	if (role_settings_lookup_active) {
		record_role_settings_lookup(3);
		if (native_role_settings_statement.role != NULL
			|| native_role_settings_statement.database != NULL)
			role_settings_bad_lookup++;
		if (role_settings_superuser_denied)
			return false;
		siglongjmp(boundary, 2); /* Stop before the real superuser/role-cache query. */
	}
	if (tsearch_lookup_active) {
		record_tsearch_lookup(1);
		if (tsearch_entry_kind != 0 && tsearch_entry_kind != 2)
			tsearch_bad_lookup++;
		if (tsearch_superuser_denied)
			return false;
		siglongjmp(boundary, 2); /* Stop before the real superuser/role-cache query. */
	}
	if (language_lookup_active) {
		language_lookups++;
		if (language_expect_binding && utility_binds == 0)
			language_unbound++;
		if (language_superuser_denied)
			return false;
		siglongjmp(boundary, 2); /* Stop before the real superuser/role-cache query. */
	}
	event_trigger_lookups++;
	event_trigger_lookup_kind = 1;
	if (event_trigger_expect_binding && utility_binds == 0)
		event_trigger_unbound++;
	if (!event_trigger_lookup_active || event_trigger_entry_kind != 0)
		event_trigger_bad_lookup++;
	if (event_trigger_superuser_denied)
		return false;
	siglongjmp(boundary, 2);
}

#include "test_cluster_fresh_event_trigger.inc"
static CreateEventTrigStmt native_event_trigger_create;
static AlterEventTrigStmt native_event_trigger_alter;
static AlterOwnerStmt native_event_trigger_owner;
static String event_trigger_owner_name;

static void
reset_native_event_trigger(bool expect_binding, unsigned kind)
{
	reset_create_preflight();
	event_trigger_lookup_active = true;
	event_trigger_expect_binding = expect_binding;
	event_trigger_superuser_denied = false;
	event_trigger_lookups = event_trigger_unbound = event_trigger_bad_lookup = 0;
	event_trigger_lookup_kind = 0;
	event_trigger_entry_kind = kind;
	native_event_trigger_create = (CreateEventTrigStmt){ .type = T_CreateEventTrigStmt,
														 .trigname = "event_trigger",
														 .eventname = "ddl_command_start" };
	native_event_trigger_alter = (AlterEventTrigStmt){ .type = T_AlterEventTrigStmt,
													   .trigname = "event_trigger",
													   .tgenabled = 'O' };
	event_trigger_owner_name = (String){ .type = T_String, .sval = "event_trigger" };
	native_event_trigger_owner = (AlterOwnerStmt){ .type = T_AlterOwnerStmt,
												   .objectType = OBJECT_EVENT_TRIGGER,
												   .object = (Node *)&event_trigger_owner_name };
}

static Node *
native_event_trigger_statement(unsigned kind)
{
	return kind == 0   ? (Node *)&native_event_trigger_create
		   : kind == 1 ? (Node *)&native_event_trigger_alter
					   : (Node *)&native_event_trigger_owner;
}

static void
native_event_trigger_entry(unsigned kind)
{
	if (kind == 0)
		(void)CreateEventTrigger(&native_event_trigger_create);
	else if (kind == 1)
		(void)AlterEventTrigger(&native_event_trigger_alter);
	else
		(void)AlterEventTriggerOwner("event_trigger", 20);
}

UT_TEST(test_native_event_triggers_bind_before_role_cache_or_catalog_lock_in_all_profiles)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_event_trigger(binds, kind);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_event_trigger_entry(kind);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(event_trigger_lookups, repeat + 1);
				UT_ASSERT_EQ(event_trigger_lookup_kind, kind == 0 ? 1 : 2);
				UT_ASSERT_EQ(event_trigger_unbound, 0);
				UT_ASSERT_EQ(event_trigger_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	event_trigger_lookup_active = false;
}

UT_TEST(test_native_event_trigger_refusals_precede_role_cache_or_lock)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_event_trigger(true, kind);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(native_event_trigger_statement(kind));
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_event_trigger_entry(kind);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0					? ERRCODE_DATA_CORRUPTED
									 : failure == 1 || failure == 2 ? ERRCODE_FEATURE_NOT_SUPPORTED
																	: ERRCODE_INTERNAL_ERROR);
			if (failure == 1 || failure == 2)
				UT_ASSERT_STR_EQ(
					error_message,
					kind == 0 ? "shared catalog creation does not support subtransactions"
							  : "shared catalog alteration does not support subtransactions");
			UT_ASSERT_EQ(event_trigger_lookups, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	event_trigger_lookup_active = false;
}

UT_TEST(test_native_event_trigger_context_guards_keep_nonshared_route)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_event_trigger(binds, kind);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_event_trigger_entry(kind);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(event_trigger_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(event_trigger_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	event_trigger_lookup_active = false;
}

UT_TEST(test_native_event_trigger_utility_warm_context_guards_preserve_binding)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_event_trigger(true, kind);
			create_sql_entry(native_event_trigger_statement(kind));
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_event_trigger_entry(kind);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(event_trigger_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(event_trigger_unbound, 0);
			UT_ASSERT_EQ(event_trigger_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	event_trigger_lookup_active = false;
}

UT_TEST(test_native_event_trigger_original_permission_refusal_survives_early_binding)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool binds = mode == 3 || mode == 7;
		int caught;

		reset_native_event_trigger(binds, 0);
		cluster_shared_catalog = (mode & 1) != 0;
		utility_storage_mode = (mode & 2) != 0;
		cluster_shared_config = (mode & 4) != 0;
		event_trigger_superuser_denied = true;
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			(void)CreateEventTrigger(&native_event_trigger_create);
		UT_ASSERT_EQ(caught, 1);
		UT_ASSERT_EQ(error_code, ERRCODE_INSUFFICIENT_PRIVILEGE);
		UT_ASSERT_STR_EQ(error_message,
						 "permission denied to create event trigger \"event_trigger\"");
		UT_ASSERT_STR_EQ(error_hint, "Must be superuser to create an event trigger.");
		UT_ASSERT_EQ(event_trigger_lookups, 1);
		UT_ASSERT_EQ(event_trigger_lookup_kind, 1);
		UT_ASSERT_EQ(event_trigger_unbound, 0);
		UT_ASSERT_EQ(event_trigger_bad_lookup, 0);
		UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
		UT_ASSERT_EQ(utility_prepares, binds ? 1 : 0);
		UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
	}
	event_trigger_lookup_active = false;
}

/* The actual dispatcher prefix stops at its original get_rolespec_oid call.
 * Later native dispatch/ACL/catalog work is deliberately outside this fixture. */
#include "test_cluster_fresh_owner.inc"
static AlterOwnerStmt native_owner_stmt;
static String native_owner_name;
static const struct {
	ObjectType type;
	const char *command;
} owner_supported[] = { { OBJECT_AGGREGATE, "ALTER AGGREGATE" },
						{ OBJECT_COLLATION, "ALTER COLLATION" },
						{ OBJECT_CONVERSION, "ALTER CONVERSION" },
						{ OBJECT_DATABASE, "ALTER DATABASE" },
						{ OBJECT_EVENT_TRIGGER, "ALTER EVENT TRIGGER" },
						{ OBJECT_FDW, "ALTER FOREIGN DATA WRAPPER" },
						{ OBJECT_FOREIGN_SERVER, "ALTER SERVER" },
						{ OBJECT_FUNCTION, "ALTER FUNCTION" },
						{ OBJECT_LANGUAGE, "ALTER LANGUAGE" },
						{ OBJECT_LARGEOBJECT, "ALTER LARGE OBJECT" },
						{ OBJECT_PROCEDURE, "ALTER PROCEDURE" },
						{ OBJECT_PUBLICATION, "ALTER PUBLICATION" },
						{ OBJECT_ROUTINE, "ALTER ROUTINE" },
						{ OBJECT_SCHEMA, "ALTER SCHEMA" },
						{ OBJECT_STATISTIC_EXT, "ALTER STATISTICS" },
						{ OBJECT_SUBSCRIPTION, "ALTER SUBSCRIPTION" },
						{ OBJECT_TABLESPACE, "ALTER TABLESPACE" },
						{ OBJECT_TSCONFIGURATION, "ALTER TEXT SEARCH CONFIGURATION" },
						{ OBJECT_TSDICTIONARY, "ALTER TEXT SEARCH DICTIONARY" } };

static void
reset_native_owner(ObjectType type, unsigned mode, bool expect_binding)
{
	reset_create_preflight();
	cluster_shared_catalog = (mode & 1) != 0;
	utility_storage_mode = (mode & 2) != 0;
	cluster_shared_config = (mode & 4) != 0;
	owner_lookup_active = true;
	owner_expect_binding = expect_binding;
	owner_lookups = owner_unbound = owner_bad_lookup = 0;
	owner_role
		= (RoleSpec){ .type = T_RoleSpec, .roletype = ROLESPEC_CSTRING, .rolename = "owner_role" };
	native_owner_name = (String){ .type = T_String, .sval = "owner_object" };
	native_owner_stmt = (AlterOwnerStmt){ .type = T_AlterOwnerStmt,
										  .objectType = type,
										  .object = (Node *)&native_owner_name,
										  .newowner = &owner_role };
}

UT_TEST(test_native_owner_dispatch_binds_before_role_lookup_in_all_profiles)
{
	for (unsigned type = 0; type < lengthof(owner_supported); type++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_owner(owner_supported[type].type, mode, binds);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)ExecAlterOwnerStmt(&native_owner_stmt);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(owner_lookups, repeat + 1);
				UT_ASSERT_EQ(owner_unbound, 0);
				UT_ASSERT_EQ(owner_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	owner_lookup_active = false;
}

UT_TEST(test_native_owner_dispatch_publication_child_and_wrong_top_precede_role_lookup)
{
	for (unsigned type = 0; type < lengthof(owner_supported); type++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_owner(owner_supported[type].type, 7, true);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry((Node *)&native_owner_stmt);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)ExecAlterOwnerStmt(&native_owner_stmt);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(owner_lookups, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	owner_lookup_active = false;
}

UT_TEST(test_native_owner_dispatch_context_guards_keep_original_names_and_nonshared_route)
{
	for (unsigned type = 0; type < lengthof(owner_supported); type++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_owner(owner_supported[type].type, mode, binds);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)ExecAlterOwnerStmt(&native_owner_stmt);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, owner_supported[type].command) != NULL);
				UT_ASSERT_EQ(owner_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(owner_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	owner_lookup_active = false;
}

UT_TEST(test_native_owner_dispatch_utility_warm_and_context_guards_preserve_binding)
{
	for (unsigned type = 0; type < lengthof(owner_supported); type++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_owner(owner_supported[type].type, 7, true);
			create_sql_entry((Node *)&native_owner_stmt);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)ExecAlterOwnerStmt(&native_owner_stmt);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(owner_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(owner_unbound, 0);
			UT_ASSERT_EQ(owner_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	owner_lookup_active = false;
}

UT_TEST(test_native_owner_dispatch_original_type_refusal_matches_utility_scope_and_precedence)
{
	const struct {
		ObjectType type;
		const char *message;
	} forbidden[]
		= { { OBJECT_MATVIEW, "materialized views is not supported in shared mode" },
			{ OBJECT_TYPE, "custom types is not supported in shared mode" },
			{ OBJECT_DOMAIN, "custom types is not supported in shared mode" },
			{ OBJECT_OPERATOR, "custom operators is not supported in shared mode" },
			{ OBJECT_OPCLASS, "custom operators is not supported in shared mode" },
			{ OBJECT_OPFAMILY, "custom operators is not supported in shared mode" },
			{ OBJECT_EXTENSION, "CREATE/DROP EXTENSION is not supported in shared mode" } };

	for (unsigned type = 0; type < lengthof(forbidden); type++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool shared = mode != 0 && mode != 2;
			int caught;

			reset_native_owner(forbidden[type].type, mode, false);
			XactReadOnly = utility_parallel = utility_recovery = utility_in_child = true;
			utility_bind_ok = false;
			if (shared) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					create_sql_entry((Node *)&native_owner_stmt);
				UT_ASSERT_EQ(caught, 1);
				UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
				UT_ASSERT_STR_EQ(error_message, forbidden[type].message);
				UT_ASSERT_STR_EQ(error_detail,
								 "PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
				reset_error();
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)ExecAlterOwnerStmt(&native_owner_stmt);
			UT_ASSERT_EQ(caught, shared ? 1 : 2);
			UT_ASSERT_EQ(error_code, shared ? ERRCODE_FEATURE_NOT_SUPPORTED : 0);
			if (shared) {
				UT_ASSERT_STR_EQ(error_message, forbidden[type].message);
				UT_ASSERT_STR_EQ(error_detail,
								 "PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
			}
			UT_ASSERT_EQ(owner_lookups, shared ? 0 : 1);
			UT_ASSERT_EQ(owner_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
		}
	}
	owner_lookup_active = false;
}

UT_TEST(test_native_owner_dispatch_pg_undo_name_refusal_matches_utility_before_binding)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool shared = mode != 0 && mode != 2;
		int caught;

		reset_native_owner(OBJECT_TABLESPACE, mode, false);
		native_owner_name.sval = "pg_undo";
		XactReadOnly = utility_parallel = utility_recovery = utility_in_child = true;
		utility_bind_ok = false;
		if (shared) {
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				create_sql_entry((Node *)&native_owner_stmt);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
			UT_ASSERT_STR_EQ(error_message, "pg_undo cannot be altered");
			UT_ASSERT_STR_EQ(
				error_hint,
				"pg_undo is a hard-coded system tablespace; owner changes are not supported.");
			UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			reset_error();
		}
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			(void)ExecAlterOwnerStmt(&native_owner_stmt);
		UT_ASSERT_EQ(caught, shared ? 1 : 2);
		UT_ASSERT_EQ(error_code, shared ? ERRCODE_FEATURE_NOT_SUPPORTED : 0);
		if (shared) {
			UT_ASSERT_STR_EQ(error_message, "pg_undo cannot be altered");
			UT_ASSERT_STR_EQ(
				error_hint,
				"pg_undo is a hard-coded system tablespace; owner changes are not supported.");
		}
		UT_ASSERT_EQ(owner_lookups, shared ? 0 : 1);
		UT_ASSERT_EQ(owner_bad_lookup, 0);
		UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
	}
	owner_lookup_active = false;
}

/* Exercise the actual LANGUAGE prefix including its original permission error.
 * Handler validation and all catalog changes remain beyond this boundary. */
#include "test_cluster_fresh_language.inc"
static CreatePLangStmt native_language_stmt;
static String language_handler_name;
static ListCell language_handler_cell;
static List language_handler_names;

static void
reset_native_language(bool expect_binding, unsigned kind)
{
	reset_create_preflight();
	language_lookup_active = true;
	language_expect_binding = expect_binding;
	language_superuser_denied = false;
	language_lookups = language_unbound = 0;
	language_handler_name = (String){ .type = T_String, .sval = "language_handler" };
	language_handler_cell = (ListCell){ .ptr_value = &language_handler_name };
	language_handler_names = (List){ .type = T_List,
									 .length = 1,
									 .max_length = 1,
									 .elements = &language_handler_cell };
	native_language_stmt = (CreatePLangStmt){ .type = T_CreatePLangStmt,
											  .replace = (kind & 1) != 0,
											  .pltrusted = (kind & 2) != 0,
											  .plname = "preflight_language",
											  .plhandler = &language_handler_names };
}

UT_TEST(test_native_language_binds_before_superuser_query_in_all_profiles)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_language(binds, kind);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)CreateProceduralLanguage(&native_language_stmt);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(language_lookups, repeat + 1);
				UT_ASSERT_EQ(language_unbound, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	language_lookup_active = false;
}

UT_TEST(test_native_language_publication_child_and_wrong_top_precede_superuser_query)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_language(true, kind);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry((Node *)&native_language_stmt);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)CreateProceduralLanguage(&native_language_stmt);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0  ? "canonical ACTIVE publication failed before shared catalog creation"
				: failure < 3 ? "shared catalog creation does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(language_lookups, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	language_lookup_active = false;
}

UT_TEST(test_native_language_context_guards_keep_nonshared_route)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_language(binds, kind);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)CreateProceduralLanguage(&native_language_stmt);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, "CREATE LANGUAGE") != NULL);
				UT_ASSERT_EQ(language_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	language_lookup_active = false;
}

UT_TEST(test_native_language_utility_warm_context_guards_preserve_binding)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_language(true, kind);
			create_sql_entry((Node *)&native_language_stmt);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)CreateProceduralLanguage(&native_language_stmt);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(language_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(language_unbound, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	language_lookup_active = false;
}

UT_TEST(test_native_language_original_permission_refusal_survives_early_binding)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_language(binds, kind);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			language_superuser_denied = true;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)CreateProceduralLanguage(&native_language_stmt);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, ERRCODE_INSUFFICIENT_PRIVILEGE);
			UT_ASSERT_STR_EQ(error_message,
							 "must be superuser to create custom procedural language");
			UT_ASSERT_EQ(language_lookups, 1);
			UT_ASSERT_EQ(language_unbound, 0);
			UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
		}
	}
	language_lookup_active = false;
}

/* The actual AM creation prefix stops at its first pg_am table_open.
 * Permission/handler checks and catalog writes remain beyond this boundary. */
#include "test_cluster_fresh_access_method.inc"
static CreateAmStmt native_access_method_stmt;
static String access_method_handler_name;
static ListCell access_method_handler_cell;
static List access_method_handler_names;

static void
reset_native_access_method(bool expect_binding, unsigned kind)
{
	reset_create_preflight();
	access_method_lookup_active = true;
	access_method_expect_binding = expect_binding;
	access_method_lookups = access_method_unbound = access_method_bad_lookup = 0;
	access_method_handler_name = (String){ .type = T_String, .sval = "am_handler" };
	access_method_handler_cell = (ListCell){ .ptr_value = &access_method_handler_name };
	access_method_handler_names = (List){ .type = T_List,
										  .length = 1,
										  .max_length = 1,
										  .elements = &access_method_handler_cell };
	native_access_method_stmt = (CreateAmStmt){ .type = T_CreateAmStmt,
												.amname = "preflight_am",
												.amtype = kind ? AMTYPE_TABLE : AMTYPE_INDEX,
												.handler_name = &access_method_handler_names };
}

UT_TEST(test_native_access_method_binds_before_catalog_lock_in_all_profiles)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_access_method(binds, kind);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)CreateAccessMethod(&native_access_method_stmt);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(access_method_lookups, repeat + 1);
				UT_ASSERT_EQ(access_method_bad_lookup, 0);
				UT_ASSERT_EQ(access_method_unbound, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	access_method_lookup_active = false;
}

UT_TEST(test_native_access_method_publication_child_and_wrong_top_precede_catalog_lock)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_access_method(true, kind);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry((Node *)&native_access_method_stmt);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)CreateAccessMethod(&native_access_method_stmt);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0  ? "canonical ACTIVE publication failed before shared catalog creation"
				: failure < 3 ? "shared catalog creation does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(access_method_lookups, 0);
			UT_ASSERT_EQ(access_method_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	access_method_lookup_active = false;
}

UT_TEST(test_native_access_method_context_guards_keep_nonshared_route)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_access_method(binds, kind);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)CreateAccessMethod(&native_access_method_stmt);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, "CREATE ACCESS METHOD") != NULL);
				UT_ASSERT_EQ(access_method_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(access_method_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	access_method_lookup_active = false;
}

UT_TEST(test_native_access_method_utility_warm_context_guards_preserve_binding)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_access_method(true, kind);
			create_sql_entry((Node *)&native_access_method_stmt);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)CreateAccessMethod(&native_access_method_stmt);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(access_method_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(access_method_bad_lookup, 0);
			UT_ASSERT_EQ(access_method_unbound, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	access_method_lookup_active = false;
}

/* These actual four native prefixes stop before their first namespace,
 * pg_proc lock or type query. Later definition/ACL/catalog work is not run. */
Oid
typenameTypeId(ParseState *pstate, const TypeName *typeName)
{
	if (!function_lookup_active)
		abort();
	function_lookups++;
	function_lookup_kind = 3;
	if (function_expect_binding && utility_binds == 0)
		function_unbound++;
	if (function_entry_kind < 5 || pstate != NULL || typeName != &function_type)
		function_bad_lookup++;
	siglongjmp(boundary, 2); /* Stop before the real type query. */
}

#include "test_cluster_fresh_function.inc"
static CreateFunctionStmt native_function_create;
static AlterFunctionStmt native_function_alter;
static CreateCastStmt native_function_cast;
static CreateTransformStmt native_function_transform;
static ObjectWithArgs function_args;
static ParseState function_parse_state;
static const char *function_commands[]
	= { "CREATE FUNCTION", "CREATE PROCEDURE", "ALTER FUNCTION",  "ALTER PROCEDURE",
		"ALTER ROUTINE",   "CREATE CAST",	   "CREATE TRANSFORM" };

static void
reset_native_function(bool expect_binding, unsigned kind)
{
	reset_create_preflight();
	function_lookup_active = true;
	function_expect_binding = expect_binding;
	function_entry_kind = kind;
	function_lookups = function_unbound = function_bad_lookup = function_lookup_kind = 0;
	function_name_node = (String){ .type = T_String, .sval = "preflight_routine" };
	function_name_cell = (ListCell){ .ptr_value = &function_name_node };
	function_names
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &function_name_cell };
	function_type = (TypeName){ .type = T_TypeName, .typeOid = INT4OID, .typemod = -1 };
	function_target_type = (TypeName){ .type = T_TypeName, .typeOid = TEXTOID, .typemod = -1 };
	function_args = (ObjectWithArgs){ .type = T_ObjectWithArgs,
									  .objname = &function_names,
									  .args_unspecified = true };
	native_function_create
		= (CreateFunctionStmt){ .type = T_CreateFunctionStmt,
								.is_procedure = kind == 1,
								.funcname = &function_names,
								.returnType = kind == 1 ? NULL : &function_type };
	native_function_alter = (AlterFunctionStmt){ .type = T_AlterFunctionStmt,
												 .objtype = kind == 2	? OBJECT_FUNCTION
															: kind == 3 ? OBJECT_PROCEDURE
																		: OBJECT_ROUTINE,
												 .func = &function_args };
	native_function_cast = (CreateCastStmt){ .type = T_CreateCastStmt,
											 .sourcetype = &function_type,
											 .targettype = &function_target_type,
											 .context = COERCION_EXPLICIT,
											 .inout = true };
	native_function_transform = (CreateTransformStmt){ .type = T_CreateTransformStmt,
													   .type_name = &function_type,
													   .lang = "sql",
													   .tosql = &function_args };
}

static Node *
native_function_statement(unsigned kind)
{
	return kind < 2	   ? (Node *)&native_function_create
		   : kind < 5  ? (Node *)&native_function_alter
		   : kind == 5 ? (Node *)&native_function_cast
					   : (Node *)&native_function_transform;
}

static void
native_function_entry(unsigned kind)
{
	if (kind < 2)
		(void)CreateFunction(&function_parse_state, &native_function_create);
	else if (kind < 5)
		(void)AlterFunction(&function_parse_state, &native_function_alter);
	else if (kind == 5)
		(void)CreateCast(&native_function_cast);
	else
		(void)CreateTransform(&native_function_transform);
}

UT_TEST(test_native_function_binds_before_catalog_lock_in_all_profiles)
{
	for (unsigned kind = 0; kind < 7; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_function(binds, kind);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_function_entry(kind);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(function_lookups, repeat + 1);
				UT_ASSERT_EQ(function_bad_lookup, 0);
				UT_ASSERT_EQ(function_lookup_kind, function_lookups ? (kind < 2	  ? 1
																	   : kind < 5 ? 2
																				  : 3)
																	: 0);
				UT_ASSERT_EQ(function_unbound, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	function_lookup_active = false;
}

UT_TEST(test_native_function_publication_child_and_wrong_top_precede_catalog_lock)
{
	for (unsigned kind = 0; kind < 7; kind++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_function(true, kind);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(native_function_statement(kind));
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_function_entry(kind);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			if (failure == 0)
				UT_ASSERT_STR_EQ(
					error_message,
					kind >= 2 && kind <= 4
						? "canonical ACTIVE publication failed before shared catalog alteration"
						: "canonical ACTIVE publication failed before shared catalog creation");
			else if (failure < 3)
				UT_ASSERT_STR_EQ(error_message,
								 kind >= 2 && kind <= 4
									 ? "shared catalog alteration does not support subtransactions"
									 : "shared catalog creation does not support subtransactions");
			else
				UT_ASSERT_STR_EQ(error_message,
								 "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(function_lookups, 0);
			UT_ASSERT_EQ(function_bad_lookup, 0);
			UT_ASSERT_EQ(function_lookup_kind, function_lookups ? (kind < 2	  ? 1
																   : kind < 5 ? 2
																			  : 3)
																: 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	function_lookup_active = false;
}

UT_TEST(test_native_function_context_guards_keep_nonshared_route)
{
	for (unsigned kind = 0; kind < 7; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_function(binds, kind);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_function_entry(kind);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, function_commands[kind]) != NULL);
				UT_ASSERT_EQ(function_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(function_bad_lookup, 0);
				UT_ASSERT_EQ(function_lookup_kind, function_lookups ? (kind < 2	  ? 1
																	   : kind < 5 ? 2
																				  : 3)
																	: 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	function_lookup_active = false;
}

UT_TEST(test_native_function_utility_warm_context_guards_preserve_binding)
{
	for (unsigned kind = 0; kind < 7; kind++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_function(true, kind);
			create_sql_entry(native_function_statement(kind));
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_function_entry(kind);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(function_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(function_bad_lookup, 0);
			UT_ASSERT_EQ(function_lookup_kind, function_lookups ? (kind < 2	  ? 1
																   : kind < 5 ? 2
																			  : 3)
																: 0);
			UT_ASSERT_EQ(function_unbound, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	function_lookup_active = false;
}

/* Actual FDW creation/alteration prefixes stop at their first catalog lock.
 * Permission/handler/validator checks and catalog writes are not executed. */
#include "test_cluster_fresh_fdw.inc"
static CreateFdwStmt native_fdw_create;
static AlterFdwStmt native_fdw_alter;
static ParseState fdw_parse_state;

static void
reset_native_fdw(bool expect_binding)
{
	reset_create_preflight();
	fdw_lookup_active = true;
	fdw_expect_binding = expect_binding;
	fdw_lookups = fdw_unbound = fdw_bad_lookup = 0;
	native_fdw_create = (CreateFdwStmt){ .type = T_CreateFdwStmt, .fdwname = "preflight_fdw" };
	native_fdw_alter = (AlterFdwStmt){ .type = T_AlterFdwStmt, .fdwname = "preflight_fdw" };
}

static Node *
native_fdw_statement(unsigned kind)
{
	return kind ? (Node *)&native_fdw_alter : (Node *)&native_fdw_create;
}

static void
native_fdw_entry(unsigned kind)
{
	if (kind)
		(void)AlterForeignDataWrapper(&fdw_parse_state, &native_fdw_alter);
	else
		(void)CreateForeignDataWrapper(&fdw_parse_state, &native_fdw_create);
}

UT_TEST(test_native_fdw_binds_before_catalog_lock_in_all_profiles)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_fdw(binds);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_fdw_entry(kind);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(fdw_lookups, repeat + 1);
				UT_ASSERT_EQ(fdw_bad_lookup, 0);
				UT_ASSERT_EQ(fdw_unbound, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	fdw_lookup_active = false;
}

UT_TEST(test_native_fdw_publication_child_and_wrong_top_precede_catalog_lock)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_fdw(true);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(native_fdw_statement(kind));
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_fdw_entry(kind);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			if (failure == 0)
				UT_ASSERT_STR_EQ(
					error_message,
					kind ? "canonical ACTIVE publication failed before shared catalog alteration"
						 : "canonical ACTIVE publication failed before shared catalog creation");
			else if (failure < 3)
				UT_ASSERT_STR_EQ(error_message,
								 kind ? "shared catalog alteration does not support subtransactions"
									  : "shared catalog creation does not support subtransactions");
			else
				UT_ASSERT_STR_EQ(error_message,
								 "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(fdw_lookups, 0);
			UT_ASSERT_EQ(fdw_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	fdw_lookup_active = false;
}

UT_TEST(test_native_fdw_context_guards_keep_nonshared_route)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_fdw(binds);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_fdw_entry(kind);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, kind ? "ALTER FOREIGN DATA WRAPPER"
														 : "CREATE FOREIGN DATA WRAPPER")
							  != NULL);
				UT_ASSERT_EQ(fdw_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(fdw_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	fdw_lookup_active = false;
}

UT_TEST(test_native_fdw_utility_warm_context_guards_preserve_binding)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_fdw(true);
			create_sql_entry(native_fdw_statement(kind));
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_fdw_entry(kind);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(fdw_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(fdw_bad_lookup, 0);
			UT_ASSERT_EQ(fdw_unbound, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	fdw_lookup_active = false;
}

/* Actual server creation/alteration prefixes share the helper extracted in
 * test_cluster_fresh_fdw.inc and stop before their first catalog lock. */
#include "test_cluster_fresh_foreign_server.inc"
static CreateForeignServerStmt native_foreign_server_create;
static AlterForeignServerStmt native_foreign_server_alter;

static void
reset_native_foreign_server(bool expect_binding, unsigned kind)
{
	reset_create_preflight();
	foreign_server_lookup_active = true;
	foreign_server_expect_binding = expect_binding;
	foreign_server_lookups = foreign_server_unbound = foreign_server_bad_lookup = 0;
	native_foreign_server_create = (CreateForeignServerStmt){ .type = T_CreateForeignServerStmt,
															  .servername = "preflight_server",
															  .fdwname = "preflight_fdw",
															  .if_not_exists = kind == 1 };
	native_foreign_server_alter = (AlterForeignServerStmt){ .type = T_AlterForeignServerStmt,
															.servername = "preflight_server",
															.has_version = true,
															.version = kind == 3 ? NULL : "2" };
}

static Node *
native_foreign_server_statement(unsigned kind)
{
	return kind >= 2 ? (Node *)&native_foreign_server_alter : (Node *)&native_foreign_server_create;
}

static void
native_foreign_server_entry(unsigned kind)
{
	if (kind >= 2)
		(void)AlterForeignServer(&native_foreign_server_alter);
	else
		(void)CreateForeignServer(&native_foreign_server_create);
}

UT_TEST(test_native_foreign_server_binds_before_catalog_lock_in_all_profiles)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_foreign_server(binds, kind);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_foreign_server_entry(kind);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(foreign_server_lookups, repeat + 1);
				UT_ASSERT_EQ(foreign_server_bad_lookup, 0);
				UT_ASSERT_EQ(foreign_server_unbound, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	foreign_server_lookup_active = false;
}

UT_TEST(test_native_foreign_server_publication_child_and_wrong_top_precede_catalog_lock)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_foreign_server(true, kind);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(native_foreign_server_statement(kind));
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_foreign_server_entry(kind);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			if (failure == 0)
				UT_ASSERT_STR_EQ(
					error_message,
					kind >= 2
						? "canonical ACTIVE publication failed before shared catalog alteration"
						: "canonical ACTIVE publication failed before shared catalog creation");
			else if (failure < 3)
				UT_ASSERT_STR_EQ(error_message,
								 kind >= 2
									 ? "shared catalog alteration does not support subtransactions"
									 : "shared catalog creation does not support subtransactions");
			else
				UT_ASSERT_STR_EQ(error_message,
								 "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(foreign_server_lookups, 0);
			UT_ASSERT_EQ(foreign_server_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	foreign_server_lookup_active = false;
}

UT_TEST(test_native_foreign_server_context_guards_keep_nonshared_route)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_foreign_server(binds, kind);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_foreign_server_entry(kind);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, kind >= 2 ? "ALTER SERVER" : "CREATE SERVER")
							  != NULL);
				UT_ASSERT_EQ(foreign_server_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(foreign_server_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	foreign_server_lookup_active = false;
}

UT_TEST(test_native_foreign_server_utility_warm_context_guards_preserve_binding)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_foreign_server(true, kind);
			create_sql_entry(native_foreign_server_statement(kind));
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_foreign_server_entry(kind);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(foreign_server_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(foreign_server_bad_lookup, 0);
			UT_ASSERT_EQ(foreign_server_unbound, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	foreign_server_lookup_active = false;
}

/* Actual mapping prefixes share the original helper from fdw.inc. CREATE
 * and ALTER stop before table_open; DROP stops before role or server lookup. */
#include "test_cluster_fresh_user_mapping.inc"
static RoleSpec native_user_mapping_role;
static CreateUserMappingStmt native_user_mapping_create;
static AlterUserMappingStmt native_user_mapping_alter;
static DropUserMappingStmt native_user_mapping_drop;
static const char *user_mapping_commands[]
	= { "CREATE USER MAPPING", "CREATE USER MAPPING", "ALTER USER MAPPING", "DROP USER MAPPING",
		"DROP USER MAPPING" };

static void
reset_native_user_mapping(bool expect_binding, unsigned kind)
{
	static const RoleSpecType roles[] = { ROLESPEC_CSTRING, ROLESPEC_PUBLIC, ROLESPEC_CURRENT_ROLE,
										  ROLESPEC_CURRENT_USER, ROLESPEC_SESSION_USER };
	unsigned op = kind / 5;

	reset_create_preflight();
	user_mapping_lookup_active = true;
	user_mapping_expect_binding = expect_binding;
	user_mapping_lookups = user_mapping_unbound = user_mapping_bad_lookup = user_mapping_lookup_kind
		= 0;
	native_user_mapping_role = (RoleSpec){ .type = T_RoleSpec,
										   .roletype = roles[kind % 5],
										   .rolename = kind % 5 == 0 ? "mapping_user" : NULL,
										   .location = -1 };
	user_mapping_expected_role = &native_user_mapping_role;
	user_mapping_expected_missing_ok = op == 4;
	user_mapping_expected_boundary = op < 3 ? 1 : kind % 5 == 1 ? 3 : 2;
	native_user_mapping_create = (CreateUserMappingStmt){ .type = T_CreateUserMappingStmt,
														  .user = &native_user_mapping_role,
														  .servername = "mapping_server",
														  .if_not_exists = op == 1 };
	native_user_mapping_alter = (AlterUserMappingStmt){ .type = T_AlterUserMappingStmt,
														.user = &native_user_mapping_role,
														.servername = "mapping_server" };
	native_user_mapping_drop = (DropUserMappingStmt){ .type = T_DropUserMappingStmt,
													  .user = &native_user_mapping_role,
													  .servername = "mapping_server",
													  .missing_ok = op == 4 };
}

static Node *
native_user_mapping_statement(unsigned kind)
{
	return kind < 10   ? (Node *)&native_user_mapping_create
		   : kind < 15 ? (Node *)&native_user_mapping_alter
					   : (Node *)&native_user_mapping_drop;
}

static void
native_user_mapping_entry(unsigned kind)
{
	if (kind < 10)
		(void)CreateUserMapping(&native_user_mapping_create);
	else if (kind < 15)
		(void)AlterUserMapping(&native_user_mapping_alter);
	else
		(void)RemoveUserMapping(&native_user_mapping_drop);
}

UT_TEST(test_native_user_mapping_binds_before_catalog_access_in_all_profiles)
{
	for (unsigned kind = 0; kind < 25; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_user_mapping(binds, kind);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_user_mapping_entry(kind);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(user_mapping_lookups, repeat + 1);
				UT_ASSERT_EQ(user_mapping_bad_lookup, 0);
				UT_ASSERT_EQ(user_mapping_lookup_kind,
							 user_mapping_lookups ? user_mapping_expected_boundary : 0);
				UT_ASSERT_EQ(user_mapping_unbound, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	user_mapping_lookup_active = false;
}

UT_TEST(test_native_user_mapping_publication_child_and_wrong_top_precede_catalog_access)
{
	for (unsigned kind = 0; kind < 25; kind++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_user_mapping(true, kind);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(native_user_mapping_statement(kind));
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_user_mapping_entry(kind);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			if (failure == 0)
				UT_ASSERT_STR_EQ(
					error_message,
					kind >= 10
						? "canonical ACTIVE publication failed before shared catalog alteration"
						: "canonical ACTIVE publication failed before shared catalog creation");
			else if (failure < 3)
				UT_ASSERT_STR_EQ(error_message,
								 kind >= 10
									 ? "shared catalog alteration does not support subtransactions"
									 : "shared catalog creation does not support subtransactions");
			else
				UT_ASSERT_STR_EQ(error_message,
								 "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(user_mapping_lookups, 0);
			UT_ASSERT_EQ(user_mapping_bad_lookup, 0);
			UT_ASSERT_EQ(user_mapping_lookup_kind,
						 user_mapping_lookups ? user_mapping_expected_boundary : 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	user_mapping_lookup_active = false;
}

UT_TEST(test_native_user_mapping_context_guards_keep_nonshared_route)
{
	for (unsigned kind = 0; kind < 25; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_user_mapping(binds, kind);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_user_mapping_entry(kind);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, user_mapping_commands[kind / 5]) != NULL);
				UT_ASSERT_EQ(user_mapping_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(user_mapping_bad_lookup, 0);
				UT_ASSERT_EQ(user_mapping_lookup_kind,
							 user_mapping_lookups ? user_mapping_expected_boundary : 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	user_mapping_lookup_active = false;
}

UT_TEST(test_native_user_mapping_utility_warm_context_guards_preserve_binding)
{
	for (unsigned kind = 0; kind < 25; kind++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_user_mapping(true, kind);
			create_sql_entry(native_user_mapping_statement(kind));
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_user_mapping_entry(kind);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(user_mapping_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(user_mapping_bad_lookup, 0);
			UT_ASSERT_EQ(user_mapping_lookup_kind,
						 user_mapping_lookups ? user_mapping_expected_boundary : 0);
			UT_ASSERT_EQ(user_mapping_unbound, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	user_mapping_lookup_active = false;
}

/* Exercise the actual ALTER prefix up to, but not through, its first lookup. */
static bool statistics_lookup_active, statistics_expect_binding;
static bool statistics_expected_missing_ok;
static unsigned statistics_lookups, statistics_unbound, statistics_bad_lookup;
static String statistics_name_node;
static ListCell statistics_name_cell;
static List statistics_names;
static AlterStatsStmt native_statistics_alter;

Oid
get_statistics_object_oid(List *names, bool missing_ok)
{
	statistics_lookups++;
	if (statistics_expect_binding && utility_binds == 0)
		statistics_unbound++;
	if (!statistics_lookup_active || names != &statistics_names
		|| missing_ok != statistics_expected_missing_ok)
		statistics_bad_lookup++;
	siglongjmp(boundary, 2); /* Stop before the real statistics object lookup. */
}

#include "test_cluster_fresh_statistics_alter.inc"

static void
reset_native_statistics_alter(bool expect_binding, unsigned kind)
{
	static const int targets[] = { -1, 0, 100, 10000, 10001 };

	reset_create_preflight();
	statistics_lookup_active = true;
	statistics_expect_binding = expect_binding;
	statistics_expected_missing_ok = kind >= 5;
	statistics_lookups = statistics_unbound = statistics_bad_lookup = 0;
	statistics_name_node = (String){ .type = T_String, .sval = "native_statistics" };
	statistics_name_cell = (ListCell){ .ptr_value = &statistics_name_node };
	statistics_names
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &statistics_name_cell };
	native_statistics_alter = (AlterStatsStmt){ .type = T_AlterStatsStmt,
												.defnames = &statistics_names,
												.stxstattarget = targets[kind % 5],
												.missing_ok = kind >= 5 };
}

UT_TEST(test_native_statistics_alter_binds_before_lookup_in_all_profiles)
{
	for (unsigned kind = 0; kind < 10; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_statistics_alter(binds, kind);
			cluster_shared_catalog = (mode & 1) != 0;
			utility_storage_mode = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)AlterStatistics(&native_statistics_alter);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(statistics_lookups, repeat + 1);
				UT_ASSERT_EQ(statistics_bad_lookup, 0);
				UT_ASSERT_EQ(statistics_unbound, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	statistics_lookup_active = false;
}

UT_TEST(test_native_statistics_alter_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned kind = 0; kind < 10; kind++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_statistics_alter(true, kind);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry((Node *)&native_statistics_alter);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)AlterStatistics(&native_statistics_alter);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT(strstr(error_message, failure == 0  ? "publication failed"
											: failure < 3 ? "does not support subtransactions"
														  : "identity mismatch")
					  != NULL);
			UT_ASSERT_EQ(statistics_lookups, 0);
			UT_ASSERT_EQ(statistics_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	statistics_lookup_active = false;
}

UT_TEST(test_native_statistics_alter_context_guards_keep_nonshared_route)
{
	for (unsigned kind = 0; kind < 10; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_statistics_alter(binds, kind);
				cluster_shared_catalog = (mode & 1) != 0;
				utility_storage_mode = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)AlterStatistics(&native_statistics_alter);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, "ALTER STATISTICS") != NULL);
				UT_ASSERT_EQ(statistics_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(statistics_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	statistics_lookup_active = false;
}

UT_TEST(test_native_statistics_alter_utility_warm_guards_preserve_binding)
{
	for (unsigned kind = 0; kind < 10; kind++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_statistics_alter(true, kind);
			create_sql_entry((Node *)&native_statistics_alter);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)AlterStatistics(&native_statistics_alter);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(statistics_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(statistics_bad_lookup, 0);
			UT_ASSERT_EQ(statistics_unbound, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	statistics_lookup_active = false;
}

UT_TEST(test_native_statistics_alter_target_error_keeps_early_binding)
{
	for (unsigned missing = 0; missing < 2; missing++) {
		for (unsigned mode = 0; mode < 3; mode++) {
			int caught;

			reset_native_statistics_alter(mode != 0, missing * 5);
			cluster_shared_catalog = mode != 0;
			native_statistics_alter.stxstattarget = -2;
			if (mode == 2)
				create_sql_entry((Node *)&native_statistics_alter);
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)AlterStatistics(&native_statistics_alter);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, ERRCODE_INVALID_PARAMETER_VALUE);
			UT_ASSERT(strstr(error_message, "statistics target -2 is too low") != NULL);
			UT_ASSERT_EQ(statistics_lookups, 0);
			UT_ASSERT_EQ(utility_xids, mode ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, mode ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, mode);
			UT_ASSERT_EQ(utility_top_xid, mode ? 400 : InvalidTransactionId);
		}
	}
	statistics_lookup_active = false;
}

/* Actual unsupported-operator entries stop before their first real lookup. */
Oid
LookupOperWithArgs(ObjectWithArgs *oper, bool noError)
{
	operator_lookups++;
	operator_lookup_kind = 2;
	if (!operator_lookup_active || operator_entry_kind < 2 || oper != &native_operator_args
		|| noError)
		operator_bad_lookup++;
	siglongjmp(boundary, 2);
}

#include "test_cluster_fresh_operator.inc"

static String operator_name_nodes[2], operator_function_name;
static ListCell operator_name_cells[2], operator_arg_cells[2], operator_parameter_cell,
	operator_function_cell;
static List operator_arg_types, operator_parameters, operator_function_names;
static TypeName operator_arg_type;
static DefElem operator_parameter;
static DefineStmt native_operator_define;
static AlterOperatorStmt native_operator_alter;
static const bool operator_shared_modes[] = { false, true, false, true, true, true, true, true };

static void
reset_native_operator(unsigned kind)
{
	reset_create_preflight();
	operator_lookup_active = true;
	operator_entry_kind = kind;
	operator_lookups = operator_bad_lookup = operator_lookup_kind = 0;
	operator_name_nodes[0] = (String){ .type = T_String, .sval = "native_schema" };
	operator_name_nodes[1] = (String){ .type = T_String, .sval = "===" };
	operator_name_cells[0] = (ListCell){ .ptr_value = &operator_name_nodes[kind % 2 ? 0 : 1] };
	operator_name_cells[1] = (ListCell){ .ptr_value = &operator_name_nodes[1] };
	operator_names = (List){ .type = T_List,
							 .length = kind % 2 ? 2 : 1,
							 .max_length = 2,
							 .elements = operator_name_cells };
	operator_arg_type = (TypeName){ .type = T_TypeName, .typeOid = INT4OID };
	operator_arg_cells[0] = (ListCell){ .ptr_value = kind == 3 ? NULL : &operator_arg_type };
	operator_arg_cells[1] = (ListCell){ .ptr_value = &operator_arg_type };
	operator_arg_types
		= (List){ .type = T_List, .length = 2, .max_length = 2, .elements = operator_arg_cells };
	native_operator_args = (ObjectWithArgs){ .type = T_ObjectWithArgs,
											 .objname = &operator_names,
											 .objargs = &operator_arg_types };
	operator_function_name = (String){ .type = T_String, .sval = "operator_function" };
	operator_function_cell = (ListCell){ .ptr_value = &operator_function_name };
	operator_function_names = (List){ .type = T_List,
									  .length = 1,
									  .max_length = 1,
									  .elements = &operator_function_cell };
	operator_parameter = (DefElem){ .type = T_DefElem,
									.defname = kind < 2 ? "function" : "restrict",
									.arg = kind < 2 ? (Node *)&operator_function_names : NULL };
	operator_parameter_cell = (ListCell){ .ptr_value = &operator_parameter };
	operator_parameters = (List){ .type = T_List,
								  .length = 1,
								  .max_length = 1,
								  .elements = &operator_parameter_cell };
	native_operator_define = (DefineStmt){ .type = T_DefineStmt,
										   .kind = OBJECT_OPERATOR,
										   .defnames = &operator_names,
										   .definition = kind % 2 ? &operator_parameters : NIL };
	native_operator_alter = (AlterOperatorStmt){ .type = T_AlterOperatorStmt,
												 .opername = &native_operator_args,
												 .options = kind % 2 ? &operator_parameters : NIL };
}

static void
native_operator_entry(unsigned kind)
{
	if (kind < 2)
		(void)DefineOperator(native_operator_define.defnames, native_operator_define.definition);
	else
		(void)AlterOperator(&native_operator_alter);
}

static void
set_native_operator_mode(unsigned mode)
{
	cluster_shared_catalog = (mode & 1) != 0;
	utility_storage_mode = (mode & 2) != 0;
	cluster_shared_config = (mode & 4) != 0;
}

UT_TEST(test_native_operator_scope_matches_utility_before_lookup_or_binding)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			int caught;

			reset_native_operator(kind);
			set_native_operator_mode(mode);
			if (operator_shared_modes[mode]) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					create_sql_entry(kind < 2 ? (Node *)&native_operator_define
											  : (Node *)&native_operator_alter);
				UT_ASSERT_EQ(caught, 1);
				UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
				UT_ASSERT_STR_EQ(error_message, "custom operators is not supported in shared mode");
				UT_ASSERT_STR_EQ(error_detail,
								 "PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
				reset_error();
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_operator_entry(kind);
			UT_ASSERT_EQ(caught, operator_shared_modes[mode] ? 1 : 2);
			UT_ASSERT_EQ(error_code,
						 operator_shared_modes[mode] ? ERRCODE_FEATURE_NOT_SUPPORTED : 0);
			if (operator_shared_modes[mode]) {
				UT_ASSERT_STR_EQ(error_message, "custom operators is not supported in shared mode");
				UT_ASSERT_STR_EQ(error_detail,
								 "PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
			}
			UT_ASSERT_EQ(operator_lookups, operator_shared_modes[mode] ? 0 : 1);
			UT_ASSERT_EQ(operator_lookup_kind, operator_shared_modes[mode] ? 0 : kind < 2 ? 1 : 2);
			UT_ASSERT_EQ(operator_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			UT_ASSERT_EQ(utility_top_xid, InvalidTransactionId);
		}
	}
	operator_lookup_active = false;
}

UT_TEST(test_native_operator_scope_refusal_precedes_context_child_top_and_publication)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			for (unsigned guard = 0; guard < 6; guard++) {
				int caught;

				reset_native_operator(kind);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				utility_in_child = guard == 3;
				if (guard == 4) {
					utility_current_xid = 401;
					utility_top_xid = 400;
				}
				utility_bind_ok = guard != 5;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_operator_entry(kind);
				UT_ASSERT_EQ(caught, operator_shared_modes[mode] ? 1 : 2);
				UT_ASSERT_EQ(error_code,
							 operator_shared_modes[mode] ? ERRCODE_FEATURE_NOT_SUPPORTED : 0);
				if (operator_shared_modes[mode]) {
					UT_ASSERT_STR_EQ(error_message,
									 "custom operators is not supported in shared mode");
					UT_ASSERT_STR_EQ(
						error_detail,
						"PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
				}
				UT_ASSERT_EQ(operator_lookups, operator_shared_modes[mode] ? 0 : 1);
				UT_ASSERT_EQ(operator_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
				UT_ASSERT_EQ(utility_top_xid, guard == 4 ? 400 : InvalidTransactionId);
			}
		}
	}
	operator_lookup_active = false;
}

UT_TEST(test_native_operator_scope_refusal_preserves_existing_top_binding)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			AlterStatsStmt supported = { .type = T_AlterStatsStmt, .stxstattarget = 100 };
			int caught;

			reset_native_operator(kind);
			create_sql_entry((Node *)&supported);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			set_native_operator_mode(mode);
			XactReadOnly = utility_parallel = utility_recovery = utility_in_child = true;
			utility_bind_ok = false;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_operator_entry(kind);
			UT_ASSERT_EQ(caught, operator_shared_modes[mode] ? 1 : 2);
			UT_ASSERT_EQ(error_code,
						 operator_shared_modes[mode] ? ERRCODE_FEATURE_NOT_SUPPORTED : 0);
			if (operator_shared_modes[mode]) {
				UT_ASSERT_STR_EQ(error_message, "custom operators is not supported in shared mode");
				UT_ASSERT_STR_EQ(error_detail,
								 "PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
			}
			UT_ASSERT_EQ(operator_lookups, operator_shared_modes[mode] ? 0 : 1);
			UT_ASSERT_EQ(operator_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	operator_lookup_active = false;
}

/* Actual class/family prefixes stop before namespace or AM cache access. */
HeapTuple
SearchSysCache1(int cacheId, Datum key1)
{
	opclass_lookups++;
	opclass_lookup_kind = 2;
	if (!opclass_lookup_active || opclass_entry_kind < 4 || cacheId != AMNAME || key1 == (Datum)0
		|| strcmp(DatumGetCString(key1), opclass_amname) != 0)
		opclass_bad_lookup++;
	siglongjmp(boundary, 2); /* Stop before the real AM cache lookup. */
}

#include "test_cluster_fresh_opclass.inc"

static String opclass_name_nodes[2];
static ListCell opclass_name_cells[2];
static TypeName opclass_datatype;
static CreateOpClassStmt native_opclass_create;
static CreateOpFamilyStmt native_opfamily_create;
static AlterOpFamilyStmt native_opfamily_alter;

static void
reset_native_opclass(unsigned kind)
{
	reset_create_preflight();
	opclass_lookup_active = true;
	opclass_entry_kind = kind;
	opclass_lookups = opclass_bad_lookup = opclass_lookup_kind = 0;
	opclass_amname = kind % 2 ? "hash" : "btree";
	opclass_name_nodes[0] = (String){ .type = T_String, .sval = "native_schema" };
	opclass_name_nodes[1] = (String){ .type = T_String, .sval = "native_index_operators" };
	opclass_name_cells[0] = (ListCell){ .ptr_value = &opclass_name_nodes[kind % 2 ? 0 : 1] };
	opclass_name_cells[1] = (ListCell){ .ptr_value = &opclass_name_nodes[1] };
	opclass_names = (List){ .type = T_List,
							.length = kind % 2 ? 2 : 1,
							.max_length = 2,
							.elements = opclass_name_cells };
	opclass_datatype = (TypeName){ .type = T_TypeName, .typeOid = INT4OID };
	native_opclass_create = (CreateOpClassStmt){ .type = T_CreateOpClassStmt,
												 .opclassname = &opclass_names,
												 .amname = opclass_amname,
												 .datatype = &opclass_datatype,
												 .isDefault = kind % 2 != 0 };
	native_opfamily_create = (CreateOpFamilyStmt){ .type = T_CreateOpFamilyStmt,
												   .opfamilyname = &opclass_names,
												   .amname = opclass_amname };
	native_opfamily_alter = (AlterOpFamilyStmt){ .type = T_AlterOpFamilyStmt,
												 .opfamilyname = &opclass_names,
												 .amname = opclass_amname,
												 .isDrop = kind == 5 };
}

static Node *
native_opclass_statement(unsigned kind)
{
	return kind < 2	  ? (Node *)&native_opclass_create
		   : kind < 4 ? (Node *)&native_opfamily_create
					  : (Node *)&native_opfamily_alter;
}

static void
native_opclass_entry(unsigned kind)
{
	if (kind < 2)
		(void)DefineOpClass(&native_opclass_create);
	else if (kind < 4)
		(void)DefineOpFamily(&native_opfamily_create);
	else
		(void)AlterOpFamily(&native_opfamily_alter);
}

UT_TEST(test_native_opclass_scope_matches_utility_before_lookup_or_binding)
{
	for (unsigned kind = 0; kind < 6; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			int caught;

			reset_native_opclass(kind);
			set_native_operator_mode(mode);
			if (operator_shared_modes[mode]) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					create_sql_entry(native_opclass_statement(kind));
				UT_ASSERT_EQ(caught, 1);
				UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
				UT_ASSERT_STR_EQ(error_message, "custom operators is not supported in shared mode");
				UT_ASSERT_STR_EQ(error_detail,
								 "PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
				reset_error();
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_opclass_entry(kind);
			UT_ASSERT_EQ(caught, operator_shared_modes[mode] ? 1 : 2);
			UT_ASSERT_EQ(error_code,
						 operator_shared_modes[mode] ? ERRCODE_FEATURE_NOT_SUPPORTED : 0);
			if (operator_shared_modes[mode]) {
				UT_ASSERT_STR_EQ(error_message, "custom operators is not supported in shared mode");
				UT_ASSERT_STR_EQ(error_detail,
								 "PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
			}
			UT_ASSERT_EQ(opclass_lookups, operator_shared_modes[mode] ? 0 : 1);
			UT_ASSERT_EQ(opclass_lookup_kind, operator_shared_modes[mode] ? 0 : kind < 4 ? 1 : 2);
			UT_ASSERT_EQ(opclass_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			UT_ASSERT_EQ(utility_top_xid, InvalidTransactionId);
		}
	}
	opclass_lookup_active = false;
}

UT_TEST(test_native_opclass_scope_refusal_precedes_context_child_top_and_publication)
{
	for (unsigned kind = 0; kind < 6; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			for (unsigned guard = 0; guard < 6; guard++) {
				int caught;

				reset_native_opclass(kind);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				utility_in_child = guard == 3;
				if (guard == 4) {
					utility_current_xid = 401;
					utility_top_xid = 400;
				}
				utility_bind_ok = guard != 5;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_opclass_entry(kind);
				UT_ASSERT_EQ(caught, operator_shared_modes[mode] ? 1 : 2);
				UT_ASSERT_EQ(error_code,
							 operator_shared_modes[mode] ? ERRCODE_FEATURE_NOT_SUPPORTED : 0);
				if (operator_shared_modes[mode]) {
					UT_ASSERT_STR_EQ(error_message,
									 "custom operators is not supported in shared mode");
					UT_ASSERT_STR_EQ(
						error_detail,
						"PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
				}
				UT_ASSERT_EQ(opclass_lookups, operator_shared_modes[mode] ? 0 : 1);
				UT_ASSERT_EQ(opclass_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
				UT_ASSERT_EQ(utility_top_xid, guard == 4 ? 400 : InvalidTransactionId);
			}
		}
	}
	opclass_lookup_active = false;
}

UT_TEST(test_native_opclass_scope_refusal_preserves_existing_top_binding)
{
	for (unsigned kind = 0; kind < 6; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			AlterStatsStmt supported = { .type = T_AlterStatsStmt, .stxstattarget = 100 };
			int caught;

			reset_native_opclass(kind);
			create_sql_entry((Node *)&supported);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			set_native_operator_mode(mode);
			XactReadOnly = utility_parallel = utility_recovery = utility_in_child = true;
			utility_bind_ok = false;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_opclass_entry(kind);
			UT_ASSERT_EQ(caught, operator_shared_modes[mode] ? 1 : 2);
			UT_ASSERT_EQ(error_code,
						 operator_shared_modes[mode] ? ERRCODE_FEATURE_NOT_SUPPORTED : 0);
			if (operator_shared_modes[mode]) {
				UT_ASSERT_STR_EQ(error_message, "custom operators is not supported in shared mode");
				UT_ASSERT_STR_EQ(error_detail,
								 "PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
			}
			UT_ASSERT_EQ(opclass_lookups, operator_shared_modes[mode] ? 0 : 1);
			UT_ASSERT_EQ(opclass_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	opclass_lookup_active = false;
}

/* The actual aggregate prefix stops before its namespace query. Argument,
 * oldstyle/replace, function, ACL and catalog work remain beyond this boundary. */
#include "test_cluster_fresh_aggregate.inc"
static ParseState aggregate_parse_state;
static String aggregate_name_nodes[2];
static ListCell aggregate_name_cells[2], aggregate_arg_cells[2], aggregate_input_cell;
static List aggregate_args, aggregate_inputs;
static TypeName aggregate_input_type;
static FunctionParameter aggregate_input;
static Integer aggregate_direct_args;
static DefineStmt native_aggregate_stmt;

static void
reset_native_aggregate(bool expect_binding, unsigned form)
{
	reset_create_preflight();
	aggregate_lookup_active = true;
	aggregate_expect_binding = expect_binding;
	aggregate_lookups = aggregate_unbound = aggregate_bad_lookup = 0;
	aggregate_name_nodes[0] = (String){ .type = T_String, .sval = "aggregate" };
	aggregate_name_nodes[1] = (String){ .type = T_String, .sval = "schema" };
	aggregate_name_cells[0].ptr_value = &aggregate_name_nodes[form & 1 ? 1 : 0];
	aggregate_name_cells[1].ptr_value = &aggregate_name_nodes[0];
	aggregate_names = (List){ .type = T_List,
							  .length = form & 1 ? 2 : 1,
							  .max_length = 2,
							  .elements = aggregate_name_cells };
	aggregate_input_type = (TypeName){ .type = T_TypeName, .typeOid = INT4OID };
	aggregate_input = (FunctionParameter){ .type = T_FunctionParameter,
										   .name = "input",
										   .argType = &aggregate_input_type,
										   .mode = FUNC_PARAM_IN };
	aggregate_input_cell.ptr_value = &aggregate_input;
	aggregate_inputs
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &aggregate_input_cell };
	aggregate_direct_args = (Integer){ .type = T_Integer, .ival = -1 };
	aggregate_arg_cells[0].ptr_value = &aggregate_inputs;
	aggregate_arg_cells[1].ptr_value = &aggregate_direct_args;
	aggregate_args
		= (List){ .type = T_List, .length = 2, .max_length = 2, .elements = aggregate_arg_cells };
	native_aggregate_stmt = (DefineStmt){ .type = T_DefineStmt,
										  .kind = OBJECT_AGGREGATE,
										  .defnames = &aggregate_names,
										  .args = form & 2 ? NIL : &aggregate_args,
										  .oldstyle = (form & 2) != 0,
										  .replace = (form & 1) != 0 };
}

static void
native_aggregate_entry(void)
{
	(void)DefineAggregate(&aggregate_parse_state, native_aggregate_stmt.defnames,
						  native_aggregate_stmt.args, native_aggregate_stmt.oldstyle,
						  native_aggregate_stmt.definition, native_aggregate_stmt.replace);
}

UT_TEST(test_native_aggregate_binds_before_namespace_query_in_all_profiles)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_aggregate(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_aggregate_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(aggregate_lookups, repeat + 1);
				UT_ASSERT_EQ(aggregate_unbound, 0);
				UT_ASSERT_EQ(aggregate_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	aggregate_lookup_active = false;
}

UT_TEST(test_native_aggregate_refuses_before_query_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_aggregate(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry((Node *)&native_aggregate_stmt);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_aggregate_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0  ? "canonical ACTIVE publication failed before shared catalog creation"
				: failure < 3 ? "shared catalog creation does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(aggregate_lookups, 0);
			UT_ASSERT_EQ(aggregate_unbound + aggregate_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	aggregate_lookup_active = false;
}

UT_TEST(test_native_aggregate_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_aggregate(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_aggregate_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(aggregate_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(aggregate_unbound + aggregate_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	aggregate_lookup_active = false;
}

UT_TEST(test_native_aggregate_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_aggregate(true, form);
			create_sql_entry((Node *)&native_aggregate_stmt);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_aggregate_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(aggregate_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(aggregate_unbound + aggregate_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	aggregate_lookup_active = false;
}

/* Actual prefixes stop at role-cache, namespace, dictionary or configuration
 * lookup boundaries. No text-search catalog operation succeeds in this fixture. */
Oid
get_ts_dict_oid(List *names, bool missing_ok)
{
	record_tsearch_lookup(3);
	if (!tsearch_lookup_active || tsearch_entry_kind != 4 || names != &tsearch_names || missing_ok)
		tsearch_bad_lookup++;
	siglongjmp(boundary, 2);
}

static HeapTuple
GetTSConfigTuple(List *names)
{
	record_tsearch_lookup(4);
	if (!tsearch_lookup_active || tsearch_entry_kind != 5 || names != &tsearch_names)
		tsearch_bad_lookup++;
	siglongjmp(boundary, 2); /* Stop before the actual private helper and its queries. */
}

#include "test_cluster_fresh_tsearch.inc"
static String tsearch_name_nodes[2];
static ListCell tsearch_name_cells[2], tsearch_dict_cell;
static List tsearch_dicts;
static DefineStmt native_tsearch_create;
static AlterTSDictionaryStmt native_tsearch_dictionary;
static AlterTSConfigurationStmt native_tsearch_configuration;
static ObjectAddress tsearch_copied;
static Node *tsearch_expected_statement;
static const ObjectType tsearch_create_types[]
	= { OBJECT_TSPARSER, OBJECT_TSDICTIONARY, OBJECT_TSTEMPLATE, OBJECT_TSCONFIGURATION };
static const AlterTSConfigType tsearch_mapping_types[]
	= { ALTER_TSCONFIG_ADD_MAPPING, ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN,
		ALTER_TSCONFIG_REPLACE_DICT, ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN,
		ALTER_TSCONFIG_DROP_MAPPING };

static void
reset_native_tsearch(bool expect_binding, unsigned kind, unsigned form)
{
	static const unsigned boundaries[] = { 1, 2, 1, 2, 3, 4 };

	reset_create_preflight();
	tsearch_lookup_active = true;
	tsearch_expect_binding = expect_binding;
	tsearch_superuser_denied = false;
	tsearch_entry_kind = kind;
	tsearch_expected_boundary = boundaries[kind];
	tsearch_lookup_kind = tsearch_lookups = tsearch_unbound = tsearch_bad_lookup = 0;
	tsearch_name_nodes[0] = (String){ .type = T_String, .sval = "text_search" };
	tsearch_name_nodes[1] = (String){ .type = T_String, .sval = "schema" };
	tsearch_name_cells[0].ptr_value = &tsearch_name_nodes[form % 2 ? 1 : 0];
	tsearch_name_cells[1].ptr_value = &tsearch_name_nodes[0];
	tsearch_names = (List){ .type = T_List,
							.length = form % 2 ? 2 : 1,
							.max_length = 2,
							.elements = tsearch_name_cells };
	tsearch_dict_cell.ptr_value = &tsearch_names;
	tsearch_dicts
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &tsearch_dict_cell };
	native_tsearch_create = (DefineStmt){ .type = T_DefineStmt,
										  .kind = tsearch_create_types[kind < 4 ? kind : 0],
										  .defnames = &tsearch_names };
	native_tsearch_dictionary
		= (AlterTSDictionaryStmt){ .type = T_AlterTSDictionaryStmt, .dictname = &tsearch_names };
	native_tsearch_configuration
		= (AlterTSConfigurationStmt){ .type = T_AlterTSConfigurationStmt,
									  .kind = tsearch_mapping_types[form / 2],
									  .cfgname = &tsearch_names,
									  .tokentype = form / 2 == 2 ? NIL : &tsearch_names,
									  .dicts = form / 2 == 4 ? NIL : &tsearch_dicts,
									  .override = form / 2 == 1,
									  .replace = form / 2 == 2 || form / 2 == 3,
									  .missing_ok = (form % 2) != 0 };
	tsearch_copied = InvalidObjectAddress;
	tsearch_expected_statement = kind < 4	 ? (Node *)&native_tsearch_create
								 : kind == 4 ? (Node *)&native_tsearch_dictionary
											 : (Node *)&native_tsearch_configuration;
}

static void
native_tsearch_entry(unsigned kind)
{
	switch (kind) {
	case 0:
		(void)DefineTSParser(native_tsearch_create.defnames, native_tsearch_create.definition);
		break;
	case 1:
		(void)DefineTSDictionary(native_tsearch_create.defnames, native_tsearch_create.definition);
		break;
	case 2:
		(void)DefineTSTemplate(native_tsearch_create.defnames, native_tsearch_create.definition);
		break;
	case 3:
		(void)DefineTSConfiguration(native_tsearch_create.defnames,
									native_tsearch_create.definition, &tsearch_copied);
		break;
	case 4:
		(void)AlterTSDictionary(&native_tsearch_dictionary);
		break;
	case 5:
		(void)AlterTSConfiguration(&native_tsearch_configuration);
		break;
	}
}

UT_TEST(test_native_tsearch_binds_before_first_query_in_all_profiles)
{
	for (unsigned kind = 0; kind < 6; kind++) {
		for (unsigned form = 0; form < 10; form++) {
			for (unsigned mode = 0; mode < 8; mode++) {
				bool binds = mode == 3 || mode == 7;
				int caught;

				reset_native_tsearch(binds, kind, form);
				set_native_operator_mode(mode);
				for (unsigned repeat = 0; repeat < 2; repeat++) {
					caught = sigsetjmp(boundary, 1);
					if (!caught)
						native_tsearch_entry(kind);
					UT_ASSERT_EQ(caught, 2);
					UT_ASSERT_EQ(error_code, 0);
					UT_ASSERT_EQ(tsearch_lookups, repeat + 1);
					UT_ASSERT_EQ(tsearch_lookup_kind, tsearch_expected_boundary);
					UT_ASSERT_EQ(tsearch_unbound + tsearch_bad_lookup, 0);
					UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
					UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
					UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
					UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
				}
			}
		}
	}
	tsearch_lookup_active = false;
}

UT_TEST(test_native_tsearch_refuses_before_queries_on_publication_child_and_top_errors)
{
	for (unsigned kind = 0; kind < 6; kind++) {
		for (unsigned form = 0; form < 10; form++) {
			for (unsigned failure = 0; failure < 5; failure++) {
				bool allocated = failure == 0 || failure == 2;
				int caught;

				reset_native_tsearch(true, kind, form);
				if (failure == 0)
					utility_bind_ok = false;
				else if (failure == 1 || failure == 2) {
					if (failure == 2)
						create_sql_entry(tsearch_expected_statement);
					utility_in_child = true;
				} else {
					utility_current_xid = 401;
					utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
				}
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_tsearch_entry(kind);
				UT_ASSERT_EQ(caught, 1);
				UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
										 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
													   : ERRCODE_INTERNAL_ERROR);
				UT_ASSERT_STR_EQ(
					error_message,
					failure == 0 ? (kind < 4 ? "canonical ACTIVE publication failed before shared "
											   "catalog creation"
											 : "canonical ACTIVE publication failed before shared "
											   "catalog alteration")
					: failure < 3
						? (kind < 4 ? "shared catalog creation does not support subtransactions"
									: "shared catalog alteration does not support subtransactions")
						: "shared catalog top-level transaction identity mismatch");
				UT_ASSERT_EQ(tsearch_lookups, 0);
				UT_ASSERT_EQ(tsearch_unbound + tsearch_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid,
							 allocated || failure == 3 ? 400 : InvalidTransactionId);
			}
		}
	}
	tsearch_lookup_active = false;
}

UT_TEST(test_native_tsearch_context_guards_keep_nonshared_routes)
{
	for (unsigned kind = 0; kind < 6; kind++) {
		for (unsigned form = 0; form < 10; form++) {
			for (unsigned mode = 0; mode < 8; mode++) {
				bool binds = mode == 3 || mode == 7;

				for (unsigned guard = 0; guard < 3; guard++) {
					int caught;

					reset_native_tsearch(binds, kind, form);
					set_native_operator_mode(mode);
					XactReadOnly = guard == 0;
					utility_parallel = guard == 1;
					utility_recovery = guard == 2;
					caught = sigsetjmp(boundary, 1);
					if (!caught)
						native_tsearch_entry(kind);
					UT_ASSERT_EQ(caught, binds ? 1 : 2);
					UT_ASSERT_EQ(error_code, !binds		  ? 0
											 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
														  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
					UT_ASSERT_EQ(tsearch_lookups, binds ? 0 : 1);
					UT_ASSERT_EQ(tsearch_unbound + tsearch_bad_lookup, 0);
					UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
				}
			}
		}
	}
	tsearch_lookup_active = false;
}

UT_TEST(test_native_tsearch_utility_warm_guards_preserve_binding)
{
	for (unsigned kind = 0; kind < 6; kind++) {
		for (unsigned form = 0; form < 10; form++) {
			for (unsigned guard = 0; guard < 4; guard++) {
				int caught;

				reset_native_tsearch(true, kind, form);
				create_sql_entry(tsearch_expected_statement);
				UT_ASSERT_EQ(utility_prepares, 1);
				UT_ASSERT_EQ(utility_xids, 1);
				UT_ASSERT_EQ(utility_binds, 1);
				XactReadOnly = guard == 1;
				utility_parallel = guard == 2;
				utility_recovery = guard == 3;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_tsearch_entry(kind);
				UT_ASSERT_EQ(caught, guard ? 1 : 2);
				UT_ASSERT_EQ(error_code, !guard		  ? 0
										 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(tsearch_lookups, guard ? 0 : 1);
				UT_ASSERT_EQ(tsearch_unbound + tsearch_bad_lookup, 0);
				UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
				UT_ASSERT_EQ(utility_xids, 1);
				UT_ASSERT_EQ(utility_binds, 1);
				UT_ASSERT_EQ(utility_top_xid, 400);
			}
		}
	}
	tsearch_lookup_active = false;
}

UT_TEST(test_native_tsearch_retains_parser_template_permission_refusals)
{
	for (unsigned kind = 0; kind <= 2; kind += 2) {
		for (unsigned form = 0; form < 10; form++) {
			for (unsigned mode = 0; mode < 8; mode++) {
				bool binds = mode == 3 || mode == 7;
				int caught;

				reset_native_tsearch(binds, kind, form);
				set_native_operator_mode(mode);
				tsearch_superuser_denied = true;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_tsearch_entry(kind);
				UT_ASSERT_EQ(caught, 1);
				UT_ASSERT_EQ(error_code, ERRCODE_INSUFFICIENT_PRIVILEGE);
				UT_ASSERT_STR_EQ(error_message,
								 kind == 0 ? "must be superuser to create text search parsers"
										   : "must be superuser to create text search templates");
				UT_ASSERT_EQ(tsearch_lookups, 1);
				UT_ASSERT_EQ(tsearch_lookup_kind, 1);
				UT_ASSERT_EQ(tsearch_unbound + tsearch_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	tsearch_lookup_active = false;
}

/* Actual publication prefixes stop before database ACL lookup or catalog lock.
 * Options, relation processing, permissions and writes remain beyond these boundaries. */
#include "test_cluster_fresh_publication.inc"
static ParseState publication_parse_state;
static String publication_option_value;
static DefElem publication_option;
static ListCell publication_option_cell, publication_object_cells[2];
static List publication_options, publication_objects;
static RangeVar publication_relation;
static PublicationTable publication_table;
static PublicationObjSpec publication_table_spec, publication_schema_spec;
static CreatePublicationStmt native_publication_create;
static AlterPublicationStmt native_publication_alter;
static Node *publication_expected_statement;

static void
reset_native_publication(bool expect_binding, unsigned form)
{
	reset_create_preflight();
	publication_lookup_active = true;
	publication_expect_binding = expect_binding;
	publication_entry_kind = form < 4 ? 0 : 1;
	publication_lookups = publication_lookup_kind = publication_unbound = publication_bad_lookup
		= 0;
	MyDatabaseId = 42;
	publication_option_value = (String){ .type = T_String, .sval = "insert" };
	publication_option = (DefElem){ .type = T_DefElem,
									.defname = "publish",
									.arg = (Node *)&publication_option_value };
	publication_option_cell.ptr_value = &publication_option;
	publication_options = (List){ .type = T_List,
								  .length = 1,
								  .max_length = 1,
								  .elements = &publication_option_cell };
	publication_relation = (RangeVar){ .type = T_RangeVar,
									   .relname = "target_table",
									   .relpersistence = RELPERSISTENCE_PERMANENT };
	publication_table
		= (PublicationTable){ .type = T_PublicationTable, .relation = &publication_relation };
	publication_table_spec = (PublicationObjSpec){ .type = T_PublicationObjSpec,
												   .pubobjtype = PUBLICATIONOBJ_TABLE,
												   .pubtable = &publication_table };
	publication_schema_spec = (PublicationObjSpec){ .type = T_PublicationObjSpec,
													.pubobjtype = PUBLICATIONOBJ_TABLES_IN_SCHEMA,
													.name = "target_schema" };
	publication_object_cells[0].ptr_value = &publication_table_spec;
	publication_object_cells[1].ptr_value = &publication_schema_spec;
	publication_objects = (List){ .type = T_List,
								  .length = 2,
								  .max_length = 2,
								  .elements = publication_object_cells };
	native_publication_create
		= (CreatePublicationStmt){ .type = T_CreatePublicationStmt,
								   .pubname
								   = form % 2 ? "second_publication" : "target_publication",
								   .options = form == 3 ? &publication_options : NIL,
								   .pubobjects = form == 2 ? &publication_objects : NIL,
								   .for_all_tables = form == 1 };
	native_publication_alter
		= (AlterPublicationStmt){ .type = T_AlterPublicationStmt,
								  .pubname = form % 2 ? "second_publication" : "target_publication",
								  .options = form == 4 ? &publication_options : NIL,
								  .pubobjects = form >= 5 ? &publication_objects : NIL,
								  .action = form == 5	? AP_AddObjects
											: form == 6 ? AP_DropObjects
														: AP_SetObjects };
	publication_expected_statement
		= form < 4 ? (Node *)&native_publication_create : (Node *)&native_publication_alter;
}

static void
native_publication_entry(unsigned form)
{
	if (form < 4)
		(void)CreatePublication(&publication_parse_state, &native_publication_create);
	else
		AlterPublication(&publication_parse_state, &native_publication_alter);
}

UT_TEST(test_native_publication_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 8; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_publication(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_publication_entry(form);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(publication_lookups, repeat + 1);
				UT_ASSERT_EQ(publication_lookup_kind, publication_entry_kind + 1);
				UT_ASSERT_EQ(publication_unbound, 0);
				UT_ASSERT_EQ(publication_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	publication_lookup_active = false;
}

UT_TEST(test_native_publication_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 8; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_publication(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(publication_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_publication_entry(form);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? (form < 4
						   ? "canonical ACTIVE publication failed before shared catalog creation"
						   : "canonical ACTIVE publication failed before shared catalog alteration")
				: failure < 3
					? (form < 4 ? "shared catalog creation does not support subtransactions"
								: "shared catalog alteration does not support subtransactions")
					: "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(publication_lookups, 0);
			UT_ASSERT_EQ(publication_unbound + publication_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	publication_lookup_active = false;
}

UT_TEST(test_native_publication_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 8; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_publication(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_publication_entry(form);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(publication_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(publication_unbound + publication_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	publication_lookup_active = false;
}

UT_TEST(test_native_publication_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 8; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_publication(true, form);
			create_sql_entry(publication_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_publication_entry(form);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(publication_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(publication_unbound + publication_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	publication_lookup_active = false;
}

/* Actual database prefixes stop before lookup or pg_database locking.
 * Settings, ownership, version checks and writes remain beyond these boundaries. */
#include "test_cluster_fresh_database_settings.inc"
static A_Const database_settings_value;
static ListCell database_settings_value_cell;
static List database_settings_values;
static VariableSetStmt database_settings_variable;
static AlterDatabaseSetStmt native_database_settings_set;
static AlterDatabaseRefreshCollStmt native_database_settings_refresh;
static Node *database_settings_expected_statement;

static void
reset_native_database_settings(bool expect_binding, unsigned form)
{
	static const VariableSetKind kinds[]
		= { VAR_SET_VALUE, VAR_SET_DEFAULT, VAR_SET_CURRENT, VAR_RESET, VAR_RESET_ALL };

	reset_create_preflight();
	database_settings_lookup_active = true;
	database_settings_expect_binding = expect_binding;
	database_settings_entry_kind = form < 5 ? 0 : 1;
	database_settings_lookups = database_settings_lookup_kind = database_settings_unbound
		= database_settings_bad_lookup = 0;
	database_settings_expected_name = form % 2 ? "second_database" : "settings_database";
	database_settings_value = (A_Const){ .type = T_A_Const, .location = -1 };
	database_settings_value.val.sval = (String){ .type = T_String, .sval = "g_preflight" };
	database_settings_value_cell.ptr_value = &database_settings_value;
	database_settings_values = (List){ .type = T_List,
									   .length = 1,
									   .max_length = 1,
									   .elements = &database_settings_value_cell };
	database_settings_variable
		= (VariableSetStmt){ .type = T_VariableSetStmt,
							 .kind = form < 5 ? kinds[form] : VAR_RESET,
							 .name = form == 4 ? NULL : "application_name",
							 .args = form == 0 ? &database_settings_values : NIL };
	native_database_settings_set
		= (AlterDatabaseSetStmt){ .type = T_AlterDatabaseSetStmt,
								  .dbname = (char *)database_settings_expected_name,
								  .setstmt = &database_settings_variable };
	native_database_settings_refresh
		= (AlterDatabaseRefreshCollStmt){ .type = T_AlterDatabaseRefreshCollStmt,
										  .dbname = (char *)database_settings_expected_name };
	database_settings_expected_statement = form < 5 ? (Node *)&native_database_settings_set
													: (Node *)&native_database_settings_refresh;
}

static void
native_database_settings_entry(unsigned form)
{
	if (form < 5)
		(void)AlterDatabaseSet(&native_database_settings_set);
	else
		(void)AlterDatabaseRefreshColl(&native_database_settings_refresh);
}

UT_TEST(test_native_database_settings_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 6; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_database_settings(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_database_settings_entry(form);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(database_settings_lookups, repeat + 1);
				UT_ASSERT_EQ(database_settings_lookup_kind, database_settings_entry_kind + 1);
				UT_ASSERT_EQ(database_settings_unbound, 0);
				UT_ASSERT_EQ(database_settings_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	database_settings_lookup_active = false;
}

UT_TEST(test_native_database_settings_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 6; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_database_settings(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(database_settings_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_database_settings_entry(form);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(database_settings_lookups, 0);
			UT_ASSERT_EQ(database_settings_unbound + database_settings_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	database_settings_lookup_active = false;
}

UT_TEST(test_native_database_settings_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 6; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_database_settings(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_database_settings_entry(form);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(database_settings_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(database_settings_unbound + database_settings_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	database_settings_lookup_active = false;
}

UT_TEST(test_native_database_settings_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 6; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_database_settings(true, form);
			create_sql_entry(database_settings_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_database_settings_entry(form);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(database_settings_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(database_settings_unbound + database_settings_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	database_settings_lookup_active = false;
}

/* Actual view prefix stops before parse analysis can query or lock sources.
 * Query semantics, permissions, options and rewrite remain beyond this boundary. */
static bool view_lookup_active, view_expect_binding;
static unsigned view_lookups, view_unbound, view_bad_lookup;
static const char *view_source_text;
static int view_location, view_length;
static SelectStmt view_select;
static A_Const view_constant;
static ResTarget view_target;
static ListCell view_target_cell;
static List view_targets;
static RangeVar view_relation;
static String view_alias;
static ListCell view_alias_cell, view_option_cell;
static List view_aliases, view_options;
static Boolean view_option_value;
static DefElem view_option;
static ViewStmt native_view_statement;
static Node *view_expected_statement;

Query *
parse_analyze_fixedparams(RawStmt *parseTree, const char *sourceText, const Oid *paramTypes,
						  int numParams, QueryEnvironment *queryEnv)
{
	view_lookups++;
	if (view_expect_binding && utility_binds == 0)
		view_unbound++;
	if (!view_lookup_active || !parseTree || !IsA(parseTree, RawStmt)
		|| parseTree->stmt != native_view_statement.query
		|| parseTree->stmt_location != view_location || parseTree->stmt_len != view_length
		|| sourceText != view_source_text || paramTypes != NULL || numParams != 0
		|| queryEnv != NULL)
		view_bad_lookup++;
	pfree(parseTree);		 /* Only this native prefix's RawStmt; its query is borrowed. */
	siglongjmp(boundary, 2); /* Stop before real parse analysis or source locks. */
}

#include "test_cluster_fresh_view.inc"

static void
reset_native_view(bool expect_binding, unsigned form)
{
	static const char *sources[]
		= { "CREATE VIEW target_view AS SELECT 1", "CREATE OR REPLACE VIEW target_view AS SELECT 1",
			"CREATE VIEW target_view AS SELECT 1 WITH LOCAL CHECK OPTION",
			"CREATE VIEW target_view(result) WITH (security_barrier=true) AS SELECT 1 WITH "
			"CASCADED CHECK OPTION" };

	reset_create_preflight();
	view_lookup_active = true;
	view_expect_binding = expect_binding;
	view_lookups = view_unbound = view_bad_lookup = 0;
	view_source_text = sources[form];
	view_location = form % 2 ? 0 : -1;
	view_length = form % 2 ? (int)strlen(view_source_text) : 0;
	view_constant = (A_Const){ .type = T_A_Const, .location = -1 };
	view_constant.val.ival = (Integer){ .type = T_Integer, .ival = 1 };
	view_target = (ResTarget){ .type = T_ResTarget, .val = (Node *)&view_constant, .location = -1 };
	view_target_cell.ptr_value = &view_target;
	view_targets
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &view_target_cell };
	view_select = (SelectStmt){ .type = T_SelectStmt, .targetList = &view_targets };
	view_relation = (RangeVar){ .type = T_RangeVar,
								.relname = "target_view",
								.relpersistence = RELPERSISTENCE_PERMANENT };
	view_alias = (String){ .type = T_String, .sval = "result" };
	view_alias_cell.ptr_value = &view_alias;
	view_aliases
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &view_alias_cell };
	view_option_value = (Boolean){ .type = T_Boolean, .boolval = true };
	view_option = (DefElem){ .type = T_DefElem,
							 .defname = "security_barrier",
							 .arg = (Node *)&view_option_value };
	view_option_cell.ptr_value = &view_option;
	view_options
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &view_option_cell };
	native_view_statement = (ViewStmt){ .type = T_ViewStmt,
										.view = &view_relation,
										.query = (Node *)&view_select,
										.replace = form == 1,
										.aliases = form == 3 ? &view_aliases : NIL,
										.options = form == 3 ? &view_options : NIL,
										.withCheckOption = form < 2	   ? NO_CHECK_OPTION
														   : form == 2 ? LOCAL_CHECK_OPTION
																	   : CASCADED_CHECK_OPTION };
	view_expected_statement = (Node *)&native_view_statement;
}

static void
native_view_entry(void)
{
	(void)DefineView(&native_view_statement, view_source_text, view_location, view_length);
}

UT_TEST(test_native_view_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_view(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_view_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(view_lookups, repeat + 1);
				UT_ASSERT_EQ(view_unbound, 0);
				UT_ASSERT_EQ(view_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	view_lookup_active = false;
}

UT_TEST(test_native_view_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_view(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(view_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_view_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0  ? "canonical ACTIVE publication failed before shared catalog creation"
				: failure < 3 ? "shared catalog creation does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(view_lookups, 0);
			UT_ASSERT_EQ(view_unbound + view_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	view_lookup_active = false;
}

UT_TEST(test_native_view_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_view(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_view_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(view_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(view_unbound + view_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	view_lookup_active = false;
}

UT_TEST(test_native_view_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_view(true, form);
			create_sql_entry(view_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_view_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(view_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(view_unbound + view_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	view_lookup_active = false;
}

/* Actual rule prefix stops before parse analysis takes the target-table lock.
 * Rule validity, actions, permissions and rewrite remain beyond this boundary. */
static bool rule_lookup_active, rule_expect_binding;
static unsigned rule_lookups, rule_unbound, rule_bad_lookup;
static const char *rule_source_text;
static RuleStmt native_rule_statement;
static Node *rule_expected_statement;
static RangeVar rule_relation;
static SelectStmt rule_select;
static A_Const rule_constant, rule_qualification;
static ResTarget rule_target;
static ListCell rule_target_cell, rule_action_cell;
static List rule_targets, rule_actions;

void
transformRuleStmt(RuleStmt *stmt, const char *queryString, List **actions, Node **whereClause)
{
	rule_lookups++;
	if (rule_expect_binding && utility_binds == 0)
		rule_unbound++;
	if (!rule_lookup_active || stmt != &native_rule_statement || queryString != rule_source_text
		|| !actions || !whereClause || (void *)actions == (void *)whereClause)
		rule_bad_lookup++;
	/* Output addresses are borrowed; do not read uninitialized locals or write them. */
	siglongjmp(boundary, 2); /* Stop before real analysis, writes or source locks. */
}

#include "test_cluster_fresh_rule.inc"

static void
reset_native_rule(bool expect_binding, unsigned form)
{
	static const char *sources[]
		= { "CREATE RULE target_rule AS ON INSERT TO target_table DO INSTEAD NOTHING",
			"CREATE OR REPLACE RULE target_rule AS ON UPDATE TO target_table WHERE true DO ALSO "
			"SELECT 1",
			"CREATE RULE target_rule AS ON DELETE TO target_table DO INSTEAD SELECT 1",
			"CREATE RULE _RETURN AS ON SELECT TO target_table DO INSTEAD SELECT 1" };
	static const CmdType events[] = { CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_SELECT };

	reset_create_preflight();
	rule_lookup_active = true;
	rule_expect_binding = expect_binding;
	rule_lookups = rule_unbound = rule_bad_lookup = 0;
	rule_source_text = sources[form];
	rule_relation = (RangeVar){ .type = T_RangeVar,
								.relname = "target_table",
								.relpersistence = RELPERSISTENCE_PERMANENT };
	rule_constant = (A_Const){ .type = T_A_Const, .location = -1 };
	rule_constant.val.ival = (Integer){ .type = T_Integer, .ival = 1 };
	rule_target = (ResTarget){ .type = T_ResTarget, .val = (Node *)&rule_constant, .location = -1 };
	rule_target_cell.ptr_value = &rule_target;
	rule_targets
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &rule_target_cell };
	rule_select = (SelectStmt){ .type = T_SelectStmt, .targetList = &rule_targets };
	rule_action_cell.ptr_value = &rule_select;
	rule_actions
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &rule_action_cell };
	rule_qualification = (A_Const){ .type = T_A_Const, .location = -1 };
	rule_qualification.val.boolval = (Boolean){ .type = T_Boolean, .boolval = true };
	native_rule_statement
		= (RuleStmt){ .type = T_RuleStmt,
					  .relation = &rule_relation,
					  .rulename = form == 3 ? "_RETURN" : "target_rule",
					  .whereClause = form == 1 ? (Node *)&rule_qualification : NULL,
					  .event = events[form],
					  .instead = form != 1,
					  .actions = form ? &rule_actions : NIL,
					  .replace = form == 1 };
	rule_expected_statement = (Node *)&native_rule_statement;
}

static void
native_rule_entry(void)
{
	(void)DefineRule(&native_rule_statement, rule_source_text);
}

UT_TEST(test_native_rule_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_rule(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_rule_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(rule_lookups, repeat + 1);
				UT_ASSERT_EQ(rule_unbound, 0);
				UT_ASSERT_EQ(rule_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	rule_lookup_active = false;
}

UT_TEST(test_native_rule_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_rule(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(rule_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_rule_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0  ? "canonical ACTIVE publication failed before shared catalog creation"
				: failure < 3 ? "shared catalog creation does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(rule_lookups, 0);
			UT_ASSERT_EQ(rule_unbound + rule_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	rule_lookup_active = false;
}

UT_TEST(test_native_rule_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_rule(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_rule_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(rule_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(rule_unbound + rule_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	rule_lookup_active = false;
}

UT_TEST(test_native_rule_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_rule(true, form);
			create_sql_entry(rule_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_rule_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(rule_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(rule_unbound + rule_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	rule_lookup_active = false;
}

/* Real private sequence types and one native prefix; all rewrite/options/SQ work
 * remain beyond the first lookup boundary. Identity forms are input shapes only. */
#include "test_cluster_fresh_sequence_alter.inc"

static ParseState sequence_alter_parse_state;
static RangeVar sequence_alter_target;
static Integer sequence_alter_option_value;
static DefElem sequence_alter_option;
static ListCell sequence_alter_option_cell;
static List sequence_alter_options;
static Node *sequence_alter_expected_statement;

static void
reset_native_sequence_alter(bool expect_binding, unsigned form)
{
	static const uint32 flags[] = { 0, RVR_MISSING_OK, 0, RVR_MISSING_OK };
	static const char *option_names[] = { "cache", "restart", "increment", "maxvalue" };

	reset_create_preflight();
	sequence_alter_lookup_active = true;
	sequence_alter_expect_binding = expect_binding;
	sequence_alter_lookups = sequence_alter_unbound = sequence_alter_bad_lookup = 0;
	sequence_alter_unexpected_callbacks = 0;
	sequence_alter_expected_flags = flags[form];
	sequence_alter_parse_state = (ParseState){ 0 };
	sequence_alter_target = (RangeVar){ .type = T_RangeVar,
										.schemaname = form < 2 ? NULL : "public",
										.relname = "target_sequence",
										.relpersistence = RELPERSISTENCE_PERMANENT };
	sequence_alter_option_value = (Integer){ .type = T_Integer, .ival = 64 };
	sequence_alter_option = (DefElem){ .type = T_DefElem,
									   .defname = (char *)option_names[form],
									   .arg = (Node *)&sequence_alter_option_value };
	sequence_alter_option_cell.ptr_value = &sequence_alter_option;
	sequence_alter_options = (List){ .type = T_List,
									 .length = 1,
									 .max_length = 1,
									 .elements = &sequence_alter_option_cell };
	native_sequence_alter_statement = (AlterSeqStmt){ .type = T_AlterSeqStmt,
													  .sequence = &sequence_alter_target,
													  .options = &sequence_alter_options,
													  .for_identity = form >= 2,
													  .missing_ok = form % 2 != 0 };
	sequence_alter_expected_statement = (Node *)&native_sequence_alter_statement;
}

static void
native_sequence_alter_entry(void)
{
	(void)AlterSequence(&sequence_alter_parse_state, &native_sequence_alter_statement);
}

UT_TEST(test_native_sequence_alter_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_sequence_alter(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_sequence_alter_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(sequence_alter_lookups, repeat + 1);
				UT_ASSERT_EQ(sequence_alter_unbound, 0);
				UT_ASSERT_EQ(sequence_alter_bad_lookup + sequence_alter_unexpected_callbacks, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	sequence_alter_lookup_active = false;
}

UT_TEST(test_native_sequence_alter_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_sequence_alter(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(sequence_alter_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_sequence_alter_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(sequence_alter_lookups, 0);
			UT_ASSERT_EQ(sequence_alter_unbound + sequence_alter_bad_lookup
							 + sequence_alter_unexpected_callbacks,
						 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	sequence_alter_lookup_active = false;
}

UT_TEST(test_native_sequence_alter_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_sequence_alter(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_sequence_alter_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(sequence_alter_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(sequence_alter_unbound + sequence_alter_bad_lookup
								 + sequence_alter_unexpected_callbacks,
							 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	sequence_alter_lookup_active = false;
}

UT_TEST(test_native_sequence_alter_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_sequence_alter(true, form);
			create_sql_entry(sequence_alter_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_sequence_alter_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(sequence_alter_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(sequence_alter_unbound + sequence_alter_bad_lookup
							 + sequence_alter_unexpected_callbacks,
						 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	sequence_alter_lookup_active = false;
}

/* Native role alteration stops before local name validation and later catalogs.
 * Role permissions, options, passwords and membership remain beyond this boundary. */
static bool role_alter_lookup_active, role_alter_expect_binding;
static unsigned role_alter_lookups, role_alter_unbound, role_alter_bad_lookup;
static ParseState role_alter_parse_state;
static RoleSpec role_alter_role;
static Boolean role_alter_option_value;
static DefElem role_alter_option;
static ListCell role_alter_option_cell;
static List role_alter_options;
static AlterRoleStmt native_role_alter_statement;
static Node *role_alter_expected_statement;

void
check_rolespec_name(const RoleSpec *role, const char *detail_msg)
{
	if (role_settings_lookup_active) {
		record_role_settings_lookup(1);
		if (role != &role_settings_role || detail_msg == NULL
			|| strcmp(detail_msg, "Cannot alter reserved roles.") != 0)
			role_settings_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before real validation or later role queries/locks. */
	}
	role_alter_lookups++;
	if (role_alter_expect_binding && utility_binds == 0)
		role_alter_unbound++;
	if (!role_alter_lookup_active || role != &role_alter_role || detail_msg == NULL
		|| strcmp(detail_msg, "Cannot alter reserved roles.") != 0)
		role_alter_bad_lookup++;
	siglongjmp(boundary, 2); /* Stop before real validation or later role queries/locks. */
}

#include "test_cluster_fresh_role_alter.inc"

static void
reset_native_role_alter(bool expect_binding, unsigned form)
{
	static const RoleSpecType types[]
		= { ROLESPEC_CSTRING, ROLESPEC_CURRENT_ROLE, ROLESPEC_CURRENT_USER, ROLESPEC_SESSION_USER };

	reset_create_preflight();
	role_alter_lookup_active = true;
	role_alter_expect_binding = expect_binding;
	role_alter_lookups = role_alter_unbound = role_alter_bad_lookup = 0;
	role_alter_parse_state = (ParseState){ 0 };
	role_alter_role = (RoleSpec){ .type = T_RoleSpec,
								  .roletype = types[form],
								  .rolename = form == 0 ? "target_role" : NULL,
								  .location = -1 };
	role_alter_option_value = (Boolean){ .type = T_Boolean, .boolval = true };
	role_alter_option = (DefElem){ .type = T_DefElem,
								   .defname = "inherit",
								   .arg = (Node *)&role_alter_option_value };
	role_alter_option_cell.ptr_value = &role_alter_option;
	role_alter_options = (List){ .type = T_List,
								 .length = 1,
								 .max_length = 1,
								 .elements = &role_alter_option_cell };
	native_role_alter_statement = (AlterRoleStmt){ .type = T_AlterRoleStmt,
												   .role = &role_alter_role,
												   .options = &role_alter_options,
												   .action = 0 };
	role_alter_expected_statement = (Node *)&native_role_alter_statement;
}

static void
native_role_alter_entry(void)
{
	(void)AlterRole(&role_alter_parse_state, &native_role_alter_statement);
}

UT_TEST(test_native_role_alter_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_role_alter(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_role_alter_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(role_alter_lookups, repeat + 1);
				UT_ASSERT_EQ(role_alter_unbound, 0);
				UT_ASSERT_EQ(role_alter_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	role_alter_lookup_active = false;
}

UT_TEST(test_native_role_alter_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_role_alter(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(role_alter_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_alter_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(role_alter_lookups, 0);
			UT_ASSERT_EQ(role_alter_unbound + role_alter_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	role_alter_lookup_active = false;
}

UT_TEST(test_native_role_alter_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_role_alter(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_role_alter_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(role_alter_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(role_alter_unbound + role_alter_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	role_alter_lookup_active = false;
}

UT_TEST(test_native_role_alter_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_role_alter(true, form);
			create_sql_entry(role_alter_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_alter_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(role_alter_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(role_alter_unbound + role_alter_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	role_alter_lookup_active = false;
}

/* These dependencies must never be reached beyond the first boundary. No role,
 * permission, lock or setting operation is stubbed as a successful native call. */
HeapTuple
get_rolespec_tuple(const RoleSpec *role)
{
	(void)role;
	role_settings_unexpected_dependencies++;
	siglongjmp(boundary, 3);
}

void
shdepLockAndCheckObject(Oid classId, Oid objectId)
{
	(void)classId;
	(void)objectId;
	role_settings_unexpected_dependencies++;
	siglongjmp(boundary, 3);
}

/* Native creation observes binding before either privilege query. */
static bool role_create_lookup_active, role_create_expect_binding, role_create_privilege_denied;
static unsigned role_create_lookups, role_create_unbound, role_create_bad_lookup;
static unsigned role_create_privilege_lookups, role_create_unexpected_dependencies;

/* Native role deletion stops at its first privilege query. */
static bool role_drop_lookup_active, role_drop_expect_binding, role_drop_privilege_denied;
static unsigned role_drop_lookups, role_drop_unbound, role_drop_bad_lookup;

bool
has_createrole_privilege(Oid roleid)
{
	if (role_create_lookup_active) {
		role_create_lookups++;
		role_create_privilege_lookups++;
		if (role_create_expect_binding && utility_binds == 0)
			role_create_unbound++;
		if (roleid != 10)
			role_create_bad_lookup++;
		if (role_create_privilege_denied)
			return false;
		role_create_unexpected_dependencies++;
		siglongjmp(boundary, 3); /* Only the original permission refusal is exercised. */
	}
	if (role_drop_lookup_active) {
		role_drop_lookups++;
		if (role_drop_expect_binding && utility_binds == 0)
			role_drop_unbound++;
		if (roleid != 10)
			role_drop_bad_lookup++;
		if (role_drop_privilege_denied)
			return false;
		siglongjmp(boundary, 2); /* Stop before the real role-cache privilege query. */
	}
	(void)roleid;
	role_settings_unexpected_dependencies++;
	siglongjmp(boundary, 3);
}

bool
is_admin_of_role(Oid member, Oid role)
{
	(void)member;
	(void)role;
	role_settings_unexpected_dependencies++;
	siglongjmp(boundary, 3);
}

void
ReleaseSysCache(HeapTuple tuple)
{
	(void)tuple;
	role_settings_unexpected_dependencies++;
	siglongjmp(boundary, 3);
}

bool
object_ownercheck(Oid classid, Oid objectid, Oid roleid)
{
	(void)classid;
	(void)objectid;
	(void)roleid;
	role_settings_unexpected_dependencies++;
	siglongjmp(boundary, 3);
}

void
AlterSetting(Oid databaseid, Oid roleid, VariableSetStmt *setstmt)
{
	(void)databaseid;
	(void)roleid;
	(void)setstmt;
	role_settings_unexpected_dependencies++;
	siglongjmp(boundary, 3);
}

#include "test_cluster_fresh_role_settings.inc"
static VariableSetStmt role_settings_variable;
static Node *role_settings_expected_statement;

static void
reset_native_role_settings(bool expect_binding, unsigned form)
{
	static const RoleSpecType types[]
		= { ROLESPEC_CSTRING, ROLESPEC_CURRENT_ROLE, ROLESPEC_CURRENT_USER, ROLESPEC_SESSION_USER };
	static const VariableSetKind kinds[]
		= { VAR_SET_VALUE, VAR_SET_DEFAULT, VAR_SET_CURRENT, VAR_RESET, VAR_RESET_ALL };

	reset_create_preflight();
	role_settings_lookup_active = true;
	role_settings_expect_binding = expect_binding;
	role_settings_superuser_denied = false;
	role_settings_lookups = role_settings_unbound = role_settings_bad_lookup = 0;
	role_settings_lookup_kind = role_settings_unexpected_dependencies = 0;
	role_settings_expected_boundary = form < 8 ? 1 : form == 8 ? 2 : 3;
	role_settings_role = (RoleSpec){ .type = T_RoleSpec,
									 .roletype = types[form % 4],
									 .rolename = form % 4 == 0 ? "settings_role" : NULL,
									 .location = -1 };
	role_settings_variable = (VariableSetStmt){ .type = T_VariableSetStmt,
												.kind = kinds[form % 5],
												.name = form % 5 == 4 ? NULL : "application_name" };
	role_settings_expected_database = form >= 4 && form < 9 ? "role_database" : NULL;
	native_role_settings_statement
		= (AlterRoleSetStmt){ .type = T_AlterRoleSetStmt,
							  .role = form < 8 ? &role_settings_role : NULL,
							  .database = (char *)role_settings_expected_database,
							  .setstmt = &role_settings_variable };
	role_settings_expected_statement = (Node *)&native_role_settings_statement;
}

static void
native_role_settings_entry(void)
{
	(void)AlterRoleSet(&native_role_settings_statement);
}

UT_TEST(test_native_role_settings_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 10; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_role_settings(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_role_settings_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(role_settings_lookups, repeat + 1);
				UT_ASSERT_EQ(role_settings_unbound, 0);
				UT_ASSERT_EQ(role_settings_bad_lookup + role_settings_unexpected_dependencies, 0);
				UT_ASSERT_EQ(role_settings_lookup_kind, role_settings_expected_boundary);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	role_settings_lookup_active = false;
}

UT_TEST(test_native_role_settings_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 10; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_role_settings(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(role_settings_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_settings_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(role_settings_lookups, 0);
			UT_ASSERT_EQ(role_settings_unbound + role_settings_bad_lookup
							 + role_settings_unexpected_dependencies,
						 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	role_settings_lookup_active = false;
}

UT_TEST(test_native_role_settings_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 10; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_role_settings(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_role_settings_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(role_settings_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(role_settings_unbound + role_settings_bad_lookup
								 + role_settings_unexpected_dependencies,
							 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	role_settings_lookup_active = false;
}

UT_TEST(test_native_role_settings_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 10; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_role_settings(true, form);
			create_sql_entry(role_settings_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_settings_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(role_settings_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(role_settings_unbound + role_settings_bad_lookup
							 + role_settings_unexpected_dependencies,
						 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	role_settings_lookup_active = false;
}

UT_TEST(test_native_role_settings_keeps_global_permission_refusal_after_binding)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool binds = mode == 3 || mode == 7;
		int caught;

		reset_native_role_settings(binds, 9);
		set_native_operator_mode(mode);
		role_settings_superuser_denied = true;
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			native_role_settings_entry();
		UT_ASSERT_EQ(caught, 1);
		UT_ASSERT_EQ(error_code, ERRCODE_INSUFFICIENT_PRIVILEGE);
		UT_ASSERT_STR_EQ(error_message, "permission denied to alter setting");
		UT_ASSERT_EQ(role_settings_lookups, 1);
		UT_ASSERT_EQ(role_settings_lookup_kind, 3);
		UT_ASSERT_EQ(role_settings_unbound + role_settings_bad_lookup
						 + role_settings_unexpected_dependencies,
					 0);
		UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
		UT_ASSERT_EQ(utility_prepares, binds ? 1 : 0);
		UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
		UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
	}
	role_settings_lookup_active = false;
}

/* The real private helper is already extracted with role settings. This
 * actual prefix includes the full original first permission refusal. */
#include "test_cluster_fresh_role_drop.inc"
static RoleSpec role_drop_roles[2];
static ListCell role_drop_role_cells[2];
static List role_drop_role_list;
static DropRoleStmt native_role_drop_statement;
static Node *role_drop_expected_statement;

static void
reset_native_role_drop(bool expect_binding, unsigned form)
{
	reset_create_preflight();
	role_drop_lookup_active = true;
	role_drop_expect_binding = expect_binding;
	role_drop_privilege_denied = false;
	role_drop_lookups = role_drop_unbound = role_drop_bad_lookup = 0;
	role_drop_roles[0] = (RoleSpec){ .type = T_RoleSpec,
									 .roletype = ROLESPEC_CSTRING,
									 .rolename = "drop_role_a",
									 .location = -1 };
	role_drop_roles[1] = (RoleSpec){ .type = T_RoleSpec,
									 .roletype = ROLESPEC_CSTRING,
									 .rolename = "drop_role_b",
									 .location = -1 };
	role_drop_role_cells[0].ptr_value = &role_drop_roles[0];
	role_drop_role_cells[1].ptr_value = &role_drop_roles[1];
	role_drop_role_list = (List){ .type = T_List,
								  .length = form < 2 ? 1 : 2,
								  .max_length = 2,
								  .elements = role_drop_role_cells };
	native_role_drop_statement = (DropRoleStmt){ .type = T_DropRoleStmt,
												 .roles = &role_drop_role_list,
												 .missing_ok = form % 2 != 0 };
	role_drop_expected_statement = (Node *)&native_role_drop_statement;
}

static void
native_role_drop_entry(void)
{
	DropRole(&native_role_drop_statement);
}

UT_TEST(test_native_role_drop_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_role_drop(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_role_drop_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(role_drop_lookups, repeat + 1);
				UT_ASSERT_EQ(role_drop_unbound, 0);
				UT_ASSERT_EQ(role_drop_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	role_drop_lookup_active = false;
}

UT_TEST(test_native_role_drop_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_role_drop(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(role_drop_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_drop_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(role_drop_lookups, 0);
			UT_ASSERT_EQ(role_drop_unbound + role_drop_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	role_drop_lookup_active = false;
}

UT_TEST(test_native_role_drop_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_role_drop(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_role_drop_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(role_drop_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(role_drop_unbound + role_drop_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	role_drop_lookup_active = false;
}

UT_TEST(test_native_role_drop_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_role_drop(true, form);
			create_sql_entry(role_drop_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_drop_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(role_drop_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(role_drop_unbound + role_drop_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	role_drop_lookup_active = false;
}

UT_TEST(test_native_role_drop_keeps_permission_refusal_after_binding)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_role_drop(binds, form);
			set_native_operator_mode(mode);
			role_drop_privilege_denied = true;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_drop_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, ERRCODE_INSUFFICIENT_PRIVILEGE);
			UT_ASSERT_STR_EQ(error_message, "permission denied to drop role");
			UT_ASSERT_EQ(role_drop_lookups, 1);
			UT_ASSERT_EQ(role_drop_unbound + role_drop_bad_lookup, 0);
			UT_ASSERT_EQ(role_settings_unexpected_dependencies, 0);
			UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
		}
	}
	role_drop_lookup_active = false;
}

/* Native reassignment stops before resolving the borrowed source-role list. */
static bool reassign_lookup_active, reassign_expect_binding;
static unsigned reassign_lookups, reassign_unbound, reassign_bad_lookup;
static List reassign_role_list;

/* No role lookup or membership operation succeeds in this native prefix. */
List *
roleSpecsToIds(List *memberNames)
{
	if (reassign_lookup_active) {
		reassign_lookups++;
		if (reassign_expect_binding && utility_binds == 0)
			reassign_unbound++;
		if (memberNames != &reassign_role_list)
			reassign_bad_lookup++;
		siglongjmp(boundary, 2); /* Stop before the real helper and its role queries. */
	}
	role_grant_lookups++;
	role_grant_lookup_kind = 2;
	if (role_grant_expect_binding && utility_binds == 0)
		role_grant_unbound++;
	if (!role_grant_lookup_active || role_grant_expected_boundary != 2
		|| memberNames != &role_grant_grantees)
		role_grant_bad_lookup++;
	siglongjmp(boundary, 2); /* Stop before the real helper and its grantee queries. */
}

bool
parse_bool(const char *value, bool *result)
{
	(void)value;
	(void)result;
	role_grant_unexpected_dependencies++;
	siglongjmp(boundary, 3); /* Options are NIL; validation is outside this fixture. */
}

#include "test_cluster_fresh_role_grant.inc"
static ParseState role_grant_parse_state;
static AccessPriv role_grant_privileges[2];
static ListCell role_grant_privilege_cells[2], role_grant_grantee_cells[2];
static List role_grant_privilege_list;
static RoleSpec role_grant_grantee_roles[2];
static GrantRoleStmt native_role_grant_statement;
static Node *role_grant_expected_statement;

static void
reset_native_role_grant(bool expect_binding, unsigned form)
{
	static const RoleSpecType types[]
		= { ROLESPEC_CSTRING, ROLESPEC_CURRENT_ROLE, ROLESPEC_CURRENT_USER, ROLESPEC_SESSION_USER };
	unsigned grantor = form % 5;

	reset_create_preflight();
	role_grant_lookup_active = true;
	role_grant_expect_binding = expect_binding;
	role_grant_lookups = role_grant_lookup_kind = role_grant_unbound = role_grant_bad_lookup
		= role_grant_unexpected_dependencies = 0;
	role_grant_expected_boundary = grantor ? 1 : 2;
	role_grant_parse_state = (ParseState){ 0 };
	role_grant_grantor = (RoleSpec){ .type = T_RoleSpec,
									 .roletype = types[grantor ? grantor - 1 : 0],
									 .rolename = grantor == 1 ? "grantor_role" : NULL,
									 .location = -1 };
	for (unsigned i = 0; i < 2; i++) {
		role_grant_privileges[i]
			= (AccessPriv){ .type = T_AccessPriv,
							.priv_name = i == 0 ? "membership_a" : "membership_b",
							.cols = NIL };
		role_grant_privilege_cells[i].ptr_value = &role_grant_privileges[i];
		role_grant_grantee_roles[i] = (RoleSpec){ .type = T_RoleSpec,
												  .roletype = ROLESPEC_CSTRING,
												  .rolename = i == 0 ? "member_a" : "member_b",
												  .location = -1 };
		role_grant_grantee_cells[i].ptr_value = &role_grant_grantee_roles[i];
	}
	role_grant_privilege_list = (List){ .type = T_List,
										.length = form % 2 ? 2 : 1,
										.max_length = 2,
										.elements = role_grant_privilege_cells };
	role_grant_grantees = (List){ .type = T_List,
								  .length = form % 2 ? 1 : 2,
								  .max_length = 2,
								  .elements = role_grant_grantee_cells };
	native_role_grant_statement
		= (GrantRoleStmt){ .type = T_GrantRoleStmt,
						   .granted_roles = &role_grant_privilege_list,
						   .grantee_roles = &role_grant_grantees,
						   .is_grant = form < 5,
						   .opt = NIL,
						   .grantor = grantor ? &role_grant_grantor : NULL,
						   .behavior = form % 2 ? DROP_CASCADE : DROP_RESTRICT };
	role_grant_expected_statement = (Node *)&native_role_grant_statement;
}

static void
native_role_grant_entry(void)
{
	GrantRole(&role_grant_parse_state, &native_role_grant_statement);
}

UT_TEST(test_native_role_grant_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 10; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_role_grant(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_role_grant_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(role_grant_lookups, repeat + 1);
				UT_ASSERT_EQ(role_grant_unbound, 0);
				UT_ASSERT_EQ(role_grant_bad_lookup + role_grant_unexpected_dependencies, 0);
				UT_ASSERT_EQ(role_grant_lookup_kind, role_grant_expected_boundary);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	role_grant_lookup_active = false;
}

UT_TEST(test_native_role_grant_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 10; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_role_grant(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(role_grant_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_grant_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(role_grant_lookups, 0);
			UT_ASSERT_EQ(
				role_grant_unbound + role_grant_bad_lookup + role_grant_unexpected_dependencies, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	role_grant_lookup_active = false;
}

UT_TEST(test_native_role_grant_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 10; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_role_grant(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_role_grant_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(role_grant_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(role_grant_unbound + role_grant_bad_lookup
								 + role_grant_unexpected_dependencies,
							 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	role_grant_lookup_active = false;
}

UT_TEST(test_native_role_grant_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 10; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_role_grant(true, form);
			create_sql_entry(role_grant_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_grant_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(role_grant_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(
				role_grant_unbound + role_grant_bad_lookup + role_grant_unexpected_dependencies, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	role_grant_lookup_active = false;
}

/* Stop before the real superuser query; false only exercises the native refusal. */
bool
superuser_arg(Oid roleid)
{
	role_create_lookups++;
	if (role_create_expect_binding && utility_binds == 0)
		role_create_unbound++;
	if (!role_create_lookup_active || roleid != 10)
		role_create_bad_lookup++;
	if (role_create_privilege_denied)
		return false;
	siglongjmp(boundary, 2);
}

/* The complete source GrantRoleOptions type precedes this extracted prefix. */
#include "test_cluster_fresh_role_create.inc"
static ParseState role_create_parse_state;
static CreateRoleStmt native_role_create_statement;
static Node *role_create_expected_statement;

static void
reset_native_role_create(bool expect_binding, unsigned form)
{
	static const RoleStmtType types[] = { ROLESTMT_ROLE, ROLESTMT_USER, ROLESTMT_GROUP };

	reset_create_preflight();
	role_create_lookup_active = true;
	role_create_expect_binding = expect_binding;
	role_create_privilege_denied = false;
	role_create_lookups = role_create_unbound = role_create_bad_lookup = 0;
	role_create_privilege_lookups = role_create_unexpected_dependencies = 0;
	role_create_parse_state = (ParseState){ 0 };
	native_role_create_statement = (CreateRoleStmt){ .type = T_CreateRoleStmt,
													 .stmt_type = types[form],
													 .role = "create_role",
													 .options = NIL };
	role_create_expected_statement = (Node *)&native_role_create_statement;
}

static void
native_role_create_entry(void)
{
	(void)CreateRole(&role_create_parse_state, &native_role_create_statement);
}

UT_TEST(test_native_role_create_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 3; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_role_create(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_role_create_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(role_create_lookups, repeat + 1);
				UT_ASSERT_EQ(role_create_unbound, 0);
				UT_ASSERT_EQ(role_create_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	UT_ASSERT_EQ(role_create_unexpected_dependencies, 0);
	role_create_lookup_active = false;
}

UT_TEST(test_native_role_create_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 3; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_role_create(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(role_create_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_create_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0  ? "canonical ACTIVE publication failed before shared catalog creation"
				: failure < 3 ? "shared catalog creation does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(role_create_lookups, 0);
			UT_ASSERT_EQ(role_create_unbound + role_create_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	UT_ASSERT_EQ(role_create_unexpected_dependencies, 0);
	role_create_lookup_active = false;
}

UT_TEST(test_native_role_create_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 3; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_role_create(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_role_create_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(role_create_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(role_create_unbound + role_create_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	UT_ASSERT_EQ(role_create_unexpected_dependencies, 0);
	role_create_lookup_active = false;
}

UT_TEST(test_native_role_create_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 3; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_role_create(true, form);
			create_sql_entry(role_create_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_create_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(role_create_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(role_create_unbound + role_create_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	UT_ASSERT_EQ(role_create_unexpected_dependencies, 0);
	role_create_lookup_active = false;
}

UT_TEST(test_native_role_create_keeps_permission_refusal_after_binding)
{
	for (unsigned form = 0; form < 3; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_role_create(binds, form);
			set_native_operator_mode(mode);
			role_create_privilege_denied = true;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_create_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, ERRCODE_INSUFFICIENT_PRIVILEGE);
			UT_ASSERT_STR_EQ(error_message, "permission denied to create role");
			UT_ASSERT_EQ(role_create_lookups, 2);
			UT_ASSERT_EQ(role_create_privilege_lookups, 1);
			UT_ASSERT_EQ(role_create_unbound + role_create_bad_lookup, 0);
			UT_ASSERT_EQ(role_create_unexpected_dependencies, 0);
			UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
		}
	}
	UT_ASSERT_EQ(role_create_unexpected_dependencies, 0);
	role_create_lookup_active = false;
}

/* Query boundary only: no successful native owner change is stubbed. */
#include "test_cluster_fresh_reassign_owned.inc"
static RoleSpec reassign_roles[2], reassign_receiver;
static ListCell reassign_role_cells[2];
static ReassignOwnedStmt native_reassign_statement;
static Node *reassign_expected_statement;

static void
reset_native_reassign(bool expect_binding, unsigned form)
{
	static const RoleSpecType types[] = { ROLESPEC_CSTRING, ROLESPEC_CSTRING, ROLESPEC_CURRENT_ROLE,
										  ROLESPEC_CURRENT_USER, ROLESPEC_SESSION_USER };

	reset_create_preflight();
	reassign_lookup_active = true;
	reassign_expect_binding = expect_binding;
	reassign_lookups = reassign_unbound = reassign_bad_lookup = 0;
	reassign_roles[0] = (RoleSpec){ .type = T_RoleSpec,
									.roletype = types[form],
									.rolename = form < 2 ? "reassign_source_a" : NULL,
									.location = -1 };
	reassign_roles[1] = (RoleSpec){ .type = T_RoleSpec,
									.roletype = ROLESPEC_CSTRING,
									.rolename = "reassign_source_b",
									.location = -1 };
	reassign_role_cells[0].ptr_value = &reassign_roles[0];
	reassign_role_cells[1].ptr_value = &reassign_roles[1];
	reassign_role_list = (List){ .type = T_List,
								 .length = form == 0 ? 2 : 1,
								 .max_length = 2,
								 .elements = reassign_role_cells };
	reassign_receiver = (RoleSpec){ .type = T_RoleSpec,
									.roletype = types[form],
									.rolename = form < 2 ? "reassign_receiver" : NULL,
									.location = -1 };
	native_reassign_statement
		= (ReassignOwnedStmt){ .type = T_ReassignOwnedStmt,
							   .roles = &reassign_role_list,
							   .newrole = form == 1 ? &reassign_roles[0] : &reassign_receiver };
	reassign_expected_statement = (Node *)&native_reassign_statement;
}

static void
native_reassign_entry(void)
{
	ReassignOwnedObjects(&native_reassign_statement);
}

UT_TEST(test_native_reassign_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 5; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_reassign(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_reassign_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(reassign_lookups, repeat + 1);
				UT_ASSERT_EQ(reassign_unbound, 0);
				UT_ASSERT_EQ(reassign_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	reassign_lookup_active = false;
}

UT_TEST(test_native_reassign_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 5; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_reassign(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(reassign_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_reassign_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(reassign_lookups, 0);
			UT_ASSERT_EQ(reassign_unbound + reassign_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	reassign_lookup_active = false;
}

UT_TEST(test_native_reassign_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 5; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_reassign(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_reassign_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				UT_ASSERT_EQ(reassign_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(reassign_unbound + reassign_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	reassign_lookup_active = false;
}

UT_TEST(test_native_reassign_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 5; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_reassign(true, form);
			create_sql_entry(reassign_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_reassign_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(reassign_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(reassign_unbound + reassign_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	reassign_lookup_active = false;
}

/* The prefix never returns a successful native role rename. */
#include "test_cluster_fresh_role_rename.inc"
static RenameStmt native_role_rename_statement;
static Node *role_rename_expected_statement;

static void
reset_native_role_rename(bool expect_binding)
{
	reset_create_preflight();
	role_rename_lookup_active = true;
	role_rename_expect_binding = expect_binding;
	role_rename_lookups = role_rename_unbound = role_rename_bad_lookup = 0;
	native_role_rename_statement = (RenameStmt){ .type = T_RenameStmt,
												 .renameType = OBJECT_ROLE,
												 .subname = "rename_source",
												 .newname = "rename_target" };
	role_rename_expected_statement = (Node *)&native_role_rename_statement;
}

static void
native_role_rename_entry(void)
{
	(void)RenameRole(native_role_rename_statement.subname, native_role_rename_statement.newname);
}

UT_TEST(test_native_role_rename_binds_before_first_query_in_all_profiles)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool binds = mode == 3 || mode == 7;
		int caught;

		reset_native_role_rename(binds);
		set_native_operator_mode(mode);
		for (unsigned repeat = 0; repeat < 2; repeat++) {
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_rename_entry();
			UT_ASSERT_EQ(caught, 2);
			UT_ASSERT_EQ(error_code, 0);
			UT_ASSERT_EQ(role_rename_lookups, repeat + 1);
			UT_ASSERT_EQ(role_rename_unbound, 0);
			UT_ASSERT_EQ(role_rename_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
			UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
		}
	}
	role_rename_lookup_active = false;
}

UT_TEST(test_native_role_rename_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned failure = 0; failure < 5; failure++) {
		bool allocated = failure == 0 || failure == 2;
		int caught;

		reset_native_role_rename(true);
		if (failure == 0)
			utility_bind_ok = false;
		else if (failure == 1 || failure == 2) {
			if (failure == 2)
				create_sql_entry(role_rename_expected_statement);
			utility_in_child = true;
		} else {
			utility_current_xid = 401;
			utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
		}
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			native_role_rename_entry();
		UT_ASSERT_EQ(caught, 1);
		UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
								 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
											   : ERRCODE_INTERNAL_ERROR);
		UT_ASSERT_STR_EQ(
			error_message,
			failure == 0  ? "canonical ACTIVE publication failed before shared catalog alteration"
			: failure < 3 ? "shared catalog alteration does not support subtransactions"
						  : "shared catalog top-level transaction identity mismatch");
		UT_ASSERT_EQ(role_rename_lookups, 0);
		UT_ASSERT_EQ(role_rename_unbound + role_rename_bad_lookup, 0);
		UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
		UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
		UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
		UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
	}
	role_rename_lookup_active = false;
}

UT_TEST(test_native_role_rename_context_guards_keep_nonshared_route)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool binds = mode == 3 || mode == 7;

		for (unsigned guard = 0; guard < 3; guard++) {
			int caught;

			reset_native_role_rename(binds);
			set_native_operator_mode(mode);
			XactReadOnly = guard == 0;
			utility_parallel = guard == 1;
			utility_recovery = guard == 2;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_role_rename_entry();
			UT_ASSERT_EQ(caught, binds ? 1 : 2);
			UT_ASSERT_EQ(error_code, !binds		  ? 0
									 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(role_rename_lookups, binds ? 0 : 1);
			UT_ASSERT_EQ(role_rename_unbound + role_rename_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
		}
	}
	role_rename_lookup_active = false;
}

UT_TEST(test_native_role_rename_utility_warm_guards_preserve_binding)
{
	for (unsigned guard = 0; guard < 4; guard++) {
		int caught;

		reset_native_role_rename(true);
		create_sql_entry(role_rename_expected_statement);
		UT_ASSERT_EQ(utility_prepares, 1);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_binds, 1);
		XactReadOnly = guard == 1;
		utility_parallel = guard == 2;
		utility_recovery = guard == 3;
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			native_role_rename_entry();
		UT_ASSERT_EQ(caught, guard ? 1 : 2);
		UT_ASSERT_EQ(error_code, !guard		  ? 0
								 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
											  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
		UT_ASSERT_EQ(role_rename_lookups, guard ? 0 : 1);
		UT_ASSERT_EQ(role_rename_unbound + role_rename_bad_lookup, 0);
		UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
		UT_ASSERT_EQ(utility_xids, 1);
		UT_ASSERT_EQ(utility_binds, 1);
		UT_ASSERT_EQ(utility_top_xid, 400);
	}
	role_rename_lookup_active = false;
}

/* Stop at the actual CREATE STATISTICS relation lock, before native queries. */
static bool statistics_create_lookup_active, statistics_create_expect_binding;
static unsigned statistics_create_lookups, statistics_create_unbound, statistics_create_bad_lookup;
static RangeVar statistics_create_relation;
static ListCell statistics_create_relation_cells[2];
static List statistics_create_relations;
static CreateStatsStmt native_statistics_create;

Relation
relation_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
	if (!statistics_create_lookup_active)
		siglongjmp(boundary, 3);
	statistics_create_lookups++;
	if (statistics_create_expect_binding && utility_binds == 0)
		statistics_create_unbound++;
	if (relation != &statistics_create_relation || lockmode != ShareUpdateExclusiveLock)
		statistics_create_bad_lookup++;
	siglongjmp(boundary, 2); /* Observe before the real relation lock or query. */
}

#include "test_cluster_fresh_statistics_create.inc"

static void
reset_native_statistics_create(bool expect_binding)
{
	reset_create_preflight();
	statistics_create_lookup_active = true;
	statistics_create_expect_binding = expect_binding;
	statistics_create_lookups = statistics_create_unbound = statistics_create_bad_lookup = 0;
	statistics_create_relation = (RangeVar){ .type = T_RangeVar, .relname = "statistics_relation" };
	statistics_create_relation_cells[0] = (ListCell){ .ptr_value = &statistics_create_relation };
	statistics_create_relation_cells[1] = (ListCell){ .ptr_value = &statistics_create_relation };
	statistics_create_relations = (List){ .type = T_List,
										  .length = 1,
										  .max_length = 2,
										  .elements = statistics_create_relation_cells };
	native_statistics_create
		= (CreateStatsStmt){ .type = T_CreateStatsStmt, .relations = &statistics_create_relations };
}

UT_TEST(test_native_statistics_create_binds_before_relation_lock_in_all_profiles)
{
	for (unsigned rights = 0; rights < 2; rights++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_statistics_create(binds);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)CreateStatistics(&native_statistics_create, rights != 0);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(statistics_create_lookups, repeat + 1);
				UT_ASSERT_EQ(statistics_create_unbound + statistics_create_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	statistics_create_lookup_active = false;
}

UT_TEST(test_native_statistics_create_refuses_before_lock_on_publication_child_and_top_errors)
{
	for (unsigned rights = 0; rights < 2; rights++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_statistics_create(true);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry((Node *)&native_statistics_create);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)CreateStatistics(&native_statistics_create, rights != 0);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0  ? "canonical ACTIVE publication failed before shared catalog creation"
				: failure < 3 ? "shared catalog creation does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(statistics_create_lookups, 0);
			UT_ASSERT_EQ(statistics_create_unbound + statistics_create_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	statistics_create_lookup_active = false;
}

UT_TEST(test_native_statistics_create_context_guards_keep_nonshared_route)
{
	for (unsigned rights = 0; rights < 2; rights++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_statistics_create(binds);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					(void)CreateStatistics(&native_statistics_create, rights != 0);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, "CREATE STATISTICS") != NULL);
				UT_ASSERT_EQ(statistics_create_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(statistics_create_unbound + statistics_create_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	statistics_create_lookup_active = false;
}

UT_TEST(test_native_statistics_create_utility_warm_guards_preserve_binding)
{
	for (unsigned rights = 0; rights < 2; rights++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_statistics_create(true);
			create_sql_entry((Node *)&native_statistics_create);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				(void)CreateStatistics(&native_statistics_create, rights != 0);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(statistics_create_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(statistics_create_unbound + statistics_create_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	statistics_create_lookup_active = false;
}

UT_TEST(test_native_statistics_create_unsupported_from_refuses_before_allocation)
{
	JoinExpr join = { .type = T_JoinExpr };
	RangeSubselect subselect = { .type = T_RangeSubselect };

	for (unsigned rights = 0; rights < 2; rights++) {
		for (unsigned form = 0; form < 4; form++) {
			for (unsigned mode = 0; mode < 8; mode++) {
				for (unsigned guard = 0; guard < 6; guard++) {
					int caught;

					reset_native_statistics_create(false);
					set_native_operator_mode(mode);
					if (form == 0)
						native_statistics_create.relations = NIL;
					else if (form == 1)
						statistics_create_relations.length = 2;
					else
						statistics_create_relation_cells[0].ptr_value
							= form == 2 ? (void *)&join : (void *)&subselect;
					XactReadOnly = guard == 1 || guard == 5;
					utility_parallel = guard == 2 || guard == 5;
					utility_recovery = guard == 3 || guard == 5;
					utility_in_child = guard == 4 || guard == 5;
					if (guard == 5) {
						utility_bind_ok = false;
						utility_current_xid = 401;
						utility_top_xid = 400;
					}
					caught = sigsetjmp(boundary, 1);
					if (!caught)
						(void)CreateStatistics(&native_statistics_create, rights != 0);
					UT_ASSERT_EQ(caught, 1);
					UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
					UT_ASSERT_STR_EQ(error_message,
									 "only a single relation is allowed in CREATE STATISTICS");
					UT_ASSERT_EQ(statistics_create_lookups, 0);
					UT_ASSERT_EQ(statistics_create_unbound + statistics_create_bad_lookup, 0);
					UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
				}
			}
		}
	}
	statistics_create_lookup_active = false;
}

/* Actual dependency entry stops before its first object lookup and lock. */
static bool depends_lookup_active, depends_expect_binding, depends_use_reference;
static unsigned depends_lookups, depends_unbound, depends_bad_lookup;
static AlterObjectDependsStmt native_depends_statement;
static RangeVar depends_relation;
static String depends_name, depends_extension;
static ListCell depends_name_cell;
static List depends_names;
static ObjectWithArgs depends_routine;
static ObjectAddress depends_reference_address;
static Node *depends_expected_statement;
static const struct {
	ObjectType type;
	const char *command;
} depends_supported[] = { { OBJECT_FUNCTION, "ALTER FUNCTION" },
						  { OBJECT_PROCEDURE, "ALTER PROCEDURE" },
						  { OBJECT_ROUTINE, "ALTER ROUTINE" },
						  { OBJECT_TRIGGER, "ALTER TRIGGER" },
						  { OBJECT_INDEX, "ALTER INDEX" } };

ObjectAddress
get_object_address_rv(ObjectType objtype, RangeVar *rel, List *object, Relation *relp,
					  LOCKMODE lockmode, bool missing_ok)
{
	if (!depends_lookup_active)
		siglongjmp(boundary, 3);
	depends_lookups++;
	if (depends_expect_binding && utility_binds == 0)
		depends_unbound++;
	if (objtype != native_depends_statement.objectType || rel != native_depends_statement.relation
		|| object != (List *)native_depends_statement.object || relp == NULL
		|| lockmode != AccessExclusiveLock || missing_ok)
		depends_bad_lookup++;
	siglongjmp(boundary, 2); /* Before the real object lookup or lock. */
}

#include "test_cluster_fresh_object_depends.inc"

static void
reset_native_depends(bool expect_binding, unsigned form)
{
	unsigned kind = form % 5;

	reset_create_preflight();
	depends_lookup_active = true;
	depends_expect_binding = expect_binding;
	depends_lookups = depends_unbound = depends_bad_lookup = 0;
	depends_relation = (RangeVar){ .type = T_RangeVar, .relname = "dependency_relation" };
	depends_name = (String){ .type = T_String, .sval = "dependency_object" };
	depends_extension = (String){ .type = T_String, .sval = "dependency_extension" };
	depends_name_cell = (ListCell){ .ptr_value = &depends_name };
	depends_names
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &depends_name_cell };
	depends_routine = (ObjectWithArgs){ .type = T_ObjectWithArgs, .objname = &depends_names };
	native_depends_statement
		= (AlterObjectDependsStmt){ .type = T_AlterObjectDependsStmt,
									.objectType = depends_supported[kind].type,
									.relation = kind >= 3 ? &depends_relation : NULL,
									.object = kind < 3	  ? (Node *)&depends_routine
											  : kind == 3 ? (Node *)&depends_names
														  : NULL,
									.extname = &depends_extension,
									.remove = ((form / 5) % 2) != 0 };
	depends_reference_address
		= (ObjectAddress){ .classId = 811, .objectId = 812, .objectSubId = 813 };
	depends_use_reference = form >= 10;
	depends_expected_statement = (Node *)&native_depends_statement;
}

static void
native_depends_entry(void)
{
	(void)ExecAlterObjectDependsStmt(&native_depends_statement,
									 depends_use_reference ? &depends_reference_address : NULL);
}

UT_TEST(test_native_depends_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 20; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_depends(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_depends_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(depends_lookups, repeat + 1);
				UT_ASSERT_EQ(depends_unbound, 0);
				UT_ASSERT_EQ(depends_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	depends_lookup_active = false;
}

UT_TEST(test_native_depends_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 20; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_depends(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(depends_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_depends_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(depends_lookups, 0);
			UT_ASSERT_EQ(depends_unbound + depends_bad_lookup, 0);
			UT_ASSERT_EQ(depends_reference_address.classId, 811);
			UT_ASSERT_EQ(depends_reference_address.objectId, 812);
			UT_ASSERT_EQ(depends_reference_address.objectSubId, 813);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	depends_lookup_active = false;
}

UT_TEST(test_native_depends_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 20; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_depends(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_depends_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, depends_supported[form % 5].command) != NULL);
				UT_ASSERT_EQ(depends_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(depends_unbound + depends_bad_lookup, 0);
				UT_ASSERT_EQ(depends_reference_address.classId, 811);
				UT_ASSERT_EQ(depends_reference_address.objectId, 812);
				UT_ASSERT_EQ(depends_reference_address.objectSubId, 813);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	depends_lookup_active = false;
}

UT_TEST(test_native_depends_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 20; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_depends(true, form);
			create_sql_entry(depends_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_depends_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(depends_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(depends_unbound + depends_bad_lookup, 0);
			UT_ASSERT_EQ(depends_reference_address.classId, 811);
			UT_ASSERT_EQ(depends_reference_address.objectId, 812);
			UT_ASSERT_EQ(depends_reference_address.objectSubId, 813);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	depends_lookup_active = false;
}

UT_TEST(test_native_depends_scope_refusals_match_utility_before_binding)
{
	static const struct {
		ObjectType type;
		const char *message;
	} forbidden[]
		= { { OBJECT_MATVIEW, "materialized views is not supported in shared mode" },
			{ OBJECT_TYPE, "custom types is not supported in shared mode" },
			{ OBJECT_DOMAIN, "custom types is not supported in shared mode" },
			{ OBJECT_OPERATOR, "custom operators is not supported in shared mode" },
			{ OBJECT_OPCLASS, "custom operators is not supported in shared mode" },
			{ OBJECT_OPFAMILY, "custom operators is not supported in shared mode" },
			{ OBJECT_EXTENSION, "CREATE/DROP EXTENSION is not supported in shared mode" } };

	for (unsigned type = 0; type < lengthof(forbidden); type++) {
		for (unsigned form = 0; form < 4; form++) {
			for (unsigned mode = 0; mode < 8; mode++) {
				bool shared = mode != 0 && mode != 2;
				int caught;

				reset_native_depends(false, form * 5);
				set_native_operator_mode(mode);
				native_depends_statement.objectType = forbidden[type].type;
				native_depends_statement.relation = &depends_relation;
				native_depends_statement.object = NULL;
				XactReadOnly = utility_parallel = utility_recovery = utility_in_child = true;
				utility_bind_ok = false;
				if (shared) {
					caught = sigsetjmp(boundary, 1);
					if (!caught)
						create_sql_entry(depends_expected_statement);
					UT_ASSERT_EQ(caught, 1);
					UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
					UT_ASSERT_STR_EQ(error_message, forbidden[type].message);
					UT_ASSERT_STR_EQ(
						error_detail,
						"PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
					UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
					reset_error();
				}
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_depends_entry();
				UT_ASSERT_EQ(caught, shared ? 1 : 2);
				UT_ASSERT_EQ(error_code, shared ? ERRCODE_FEATURE_NOT_SUPPORTED : 0);
				if (shared) {
					UT_ASSERT_STR_EQ(error_message, forbidden[type].message);
					UT_ASSERT_STR_EQ(
						error_detail,
						"PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
				}
				UT_ASSERT_EQ(depends_lookups, shared ? 0 : 1);
				UT_ASSERT_EQ(depends_unbound + depends_bad_lookup, 0);
				UT_ASSERT_EQ(depends_reference_address.classId, 811);
				UT_ASSERT_EQ(depends_reference_address.objectId, 812);
				UT_ASSERT_EQ(depends_reference_address.objectSubId, 813);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	depends_lookup_active = false;
}

static void
observe_native_acl_grant_target(ObjectType type, List *objects, unsigned kind)
{
	if (!acl_grant_lookup_active)
		siglongjmp(boundary, 3);
	acl_grant_lookups++;
	acl_grant_lookup_kind = kind;
	if (acl_grant_expect_binding && utility_binds == 0)
		acl_grant_unbound++;
	if (acl_grant_expected_boundary != kind || type != native_acl_grant_statement.objtype
		|| objects != &acl_grant_objects)
		acl_grant_bad_lookup++;
}

static List *
objectNamesToOids(ObjectType objtype, List *objnames, bool is_grant)
{
	observe_native_acl_grant_target(objtype, objnames, 2);
	if (is_grant != native_acl_grant_statement.is_grant)
		acl_grant_bad_lookup++;
	siglongjmp(boundary, 2); /* Before actual object resolution. */
}

static List *
objectsInSchemaToOids(ObjectType objtype, List *nspnames)
{
	observe_native_acl_grant_target(objtype, nspnames, 3);
	siglongjmp(boundary, 2); /* Before actual schema resolution. */
}

#include "test_cluster_fresh_acl_grant.inc"
static Node *acl_grant_expected_statement;
static RangeVar acl_grant_relation;
static String acl_grant_name;
static Integer acl_grant_large_object;
static ListCell acl_grant_name_cell, acl_grant_object_cell;
static List acl_grant_names;
static ObjectWithArgs acl_grant_routine;
static const ObjectType acl_grant_targets[]
	= { OBJECT_TABLE,	 OBJECT_SEQUENCE,	OBJECT_FDW,			OBJECT_FOREIGN_SERVER,
		OBJECT_FUNCTION, OBJECT_PROCEDURE,	OBJECT_ROUTINE,		OBJECT_DATABASE,
		OBJECT_DOMAIN,	 OBJECT_LANGUAGE,	OBJECT_LARGEOBJECT, OBJECT_PARAMETER_ACL,
		OBJECT_SCHEMA,	 OBJECT_TABLESPACE, OBJECT_TYPE,		OBJECT_TABLE,
		OBJECT_SEQUENCE, OBJECT_FUNCTION,	OBJECT_PROCEDURE,	OBJECT_ROUTINE };

static void
reset_native_acl_grant(bool expect_binding, unsigned form)
{
	unsigned target = form % 20;
	unsigned action = (form / 20) % 4;
	bool explicit_grantor = form >= 80;
	ObjectType type = acl_grant_targets[target];
	Node *object;

	reset_create_preflight();
	acl_grant_lookup_active = true;
	acl_grant_expect_binding = expect_binding;
	acl_grant_mismatched_grantor = false;
	acl_grant_lookups = acl_grant_lookup_kind = acl_grant_unbound = acl_grant_bad_lookup = 0;
	acl_grant_expected_boundary = explicit_grantor ? 1 : target < 15 ? 2 : 3;
	acl_grant_grantor = (RoleSpec){ .type = T_RoleSpec,
									.roletype = ROLESPEC_CSTRING,
									.rolename = "grant_current_user" };
	acl_grant_relation = (RangeVar){ .type = T_RangeVar, .relname = "grant_relation" };
	acl_grant_name = (String){ .type = T_String, .sval = "grant_object" };
	acl_grant_large_object = (Integer){ .type = T_Integer, .ival = 701 };
	acl_grant_name_cell = (ListCell){ .ptr_value = &acl_grant_name };
	acl_grant_names
		= (List){ .type = T_List, .length = 1, .max_length = 1, .elements = &acl_grant_name_cell };
	acl_grant_routine = (ObjectWithArgs){ .type = T_ObjectWithArgs, .objname = &acl_grant_names };
	if (target >= 15)
		object = (Node *)&acl_grant_name;
	else if (type == OBJECT_TABLE || type == OBJECT_SEQUENCE)
		object = (Node *)&acl_grant_relation;
	else if (type == OBJECT_FUNCTION || type == OBJECT_PROCEDURE || type == OBJECT_ROUTINE)
		object = (Node *)&acl_grant_routine;
	else if (type == OBJECT_DOMAIN || type == OBJECT_TYPE)
		object = (Node *)&acl_grant_names;
	else if (type == OBJECT_LARGEOBJECT)
		object = (Node *)&acl_grant_large_object;
	else
		object = (Node *)&acl_grant_name;
	acl_grant_object_cell = (ListCell){ .ptr_value = object };
	acl_grant_objects = (List){ .type = T_List,
								.length = 1,
								.max_length = 1,
								.elements = &acl_grant_object_cell };
	native_acl_grant_statement
		= (GrantStmt){ .type = T_GrantStmt,
					   .is_grant = action < 2,
					   .targtype = target < 15 ? ACL_TARGET_OBJECT : ACL_TARGET_ALL_IN_SCHEMA,
					   .objtype = type,
					   .objects = &acl_grant_objects,
					   .grant_option = action == 1 || action == 3,
					   .grantor = explicit_grantor ? &acl_grant_grantor : NULL,
					   .behavior = action == 3 ? DROP_CASCADE : DROP_RESTRICT };
	acl_grant_expected_statement = (Node *)&native_acl_grant_statement;
}

static void
native_acl_grant_entry(void)
{
	ExecuteGrantStmt(&native_acl_grant_statement);
}

UT_TEST(test_native_acl_grant_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 160; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_acl_grant(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_acl_grant_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(acl_grant_lookups, repeat + 1);
				UT_ASSERT_EQ(acl_grant_lookup_kind,
							 acl_grant_lookups ? acl_grant_expected_boundary : 0);
				UT_ASSERT_EQ(acl_grant_unbound, 0);
				UT_ASSERT_EQ(acl_grant_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	acl_grant_lookup_active = false;
}

UT_TEST(test_native_acl_grant_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 160; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_acl_grant(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(acl_grant_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_acl_grant_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(acl_grant_lookups, 0);
			UT_ASSERT_EQ(acl_grant_lookup_kind,
						 acl_grant_lookups ? acl_grant_expected_boundary : 0);
			UT_ASSERT_EQ(acl_grant_unbound + acl_grant_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	acl_grant_lookup_active = false;
}

UT_TEST(test_native_acl_grant_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 160; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_acl_grant(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_acl_grant_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message,
									 native_acl_grant_statement.is_grant ? "GRANT" : "REVOKE")
							  != NULL);
				UT_ASSERT_EQ(acl_grant_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(acl_grant_lookup_kind,
							 acl_grant_lookups ? acl_grant_expected_boundary : 0);
				UT_ASSERT_EQ(acl_grant_unbound + acl_grant_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	acl_grant_lookup_active = false;
}

UT_TEST(test_native_acl_grant_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 160; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_acl_grant(true, form);
			create_sql_entry(acl_grant_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_acl_grant_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(acl_grant_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(acl_grant_lookup_kind,
						 acl_grant_lookups ? acl_grant_expected_boundary : 0);
			UT_ASSERT_EQ(acl_grant_unbound + acl_grant_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	acl_grant_lookup_active = false;
}

UT_TEST(test_native_acl_grant_keeps_original_grantor_mismatch_refusal)
{
	for (unsigned form = 80; form < 160; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_acl_grant(binds, form);
			set_native_operator_mode(mode);
			acl_grant_mismatched_grantor = true;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_acl_grant_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
			UT_ASSERT_STR_EQ(error_message, "grantor must be current user");
			UT_ASSERT_EQ(acl_grant_lookups, 1);
			UT_ASSERT_EQ(acl_grant_lookup_kind, 1);
			UT_ASSERT_EQ(acl_grant_unbound + acl_grant_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
		}
	}
	acl_grant_lookup_active = false;
}

/* A local one-cell PUBLIC list model; no role/catalog/ACL lookup succeeds. */
List *
lappend_oid(List *list, Oid datum)
{
	if (!default_acl_lookup_active || default_acl_expected_boundary == 1 || list != NIL
		|| datum != ACL_ID_PUBLIC || default_acl_local_appends != 0)
		siglongjmp(boundary, 3);
	default_acl_local_appends++;
	default_acl_public_oid_cell = (ListCell){ .oid_value = datum };
	default_acl_public_oids = (List){ .type = T_OidList,
									  .length = 1,
									  .max_length = 1,
									  .elements = &default_acl_public_oid_cell };
	return &default_acl_public_oids;
}

bool
has_privs_of_role(Oid member, Oid role)
{
	(void)member;
	(void)role;
	default_acl_unexpected_dependencies++;
	siglongjmp(boundary, 3); /* No role lookup is allowed to return successfully. */
}

#include "test_cluster_fresh_default_privileges.inc"
static AlterDefaultPrivilegesStmt native_default_acl_statement;
static GrantStmt default_acl_action;
static Node *default_acl_expected_statement;
static List default_acl_grantees, default_acl_roles, default_acl_schemas, default_acl_options;
static List default_acl_privileges;
static ListCell default_acl_grantee_cell, default_acl_role_cell, default_acl_schema_cell;
static ListCell default_acl_option_cells[2], default_acl_privilege_cell;
static String default_acl_schema;
static DefElem default_acl_role_option, default_acl_schema_option;
static AccessPriv default_acl_privilege;
static const ObjectType default_acl_targets[]
	= { OBJECT_TABLE, OBJECT_SEQUENCE, OBJECT_FUNCTION, OBJECT_TYPE, OBJECT_SCHEMA };
static const char *const default_acl_privilege_names[]
	= { "select", "usage", "execute", "usage", "create" };
static const AclMode default_acl_privilege_bits[]
	= { ACL_SELECT, ACL_USAGE, ACL_EXECUTE, ACL_USAGE, ACL_CREATE };
static bool default_acl_named_privilege;
static unsigned default_acl_target;

static void
SetDefaultACLsInSchemas(InternalDefaultACL *iacls, List *nspnames)
{
	if (!default_acl_lookup_active)
		siglongjmp(boundary, 3);
	default_acl_lookups++;
	default_acl_lookup_kind = 3;
	if (default_acl_expect_binding && utility_binds == 0)
		default_acl_unbound++;
	if (default_acl_expected_boundary != 3 || default_acl_local_appends != 1 || iacls->roleid != 10
		|| iacls->objtype != default_acl_action.objtype
		|| iacls->is_grant != default_acl_action.is_grant
		|| iacls->grant_option != default_acl_action.grant_option
		|| iacls->behavior != default_acl_action.behavior
		|| iacls->all_privs == default_acl_named_privilege
		|| iacls->privileges
			   != (default_acl_named_privilege ? default_acl_privilege_bits[default_acl_target]
											   : ACL_NO_RIGHTS)
		|| iacls->grantees != &default_acl_public_oids || list_length(iacls->grantees) != 1
		|| linitial_oid(iacls->grantees) != ACL_ID_PUBLIC
		|| nspnames != (native_default_acl_statement.options != NIL ? &default_acl_schemas : NIL))
		default_acl_bad_lookup++;
	siglongjmp(boundary, 2); /* Before actual schema/default-ACL queries or mutation. */
}

static void
reset_native_default_acl(bool expect_binding, unsigned form)
{
	unsigned action = (form / 5) % 4;
	unsigned route = (form / 20) % 3;
	bool schema = (form / 60) % 2;

	reset_create_preflight();
	default_acl_lookup_active = true;
	default_acl_expect_binding = expect_binding;
	default_acl_lookups = default_acl_lookup_kind = default_acl_unbound = default_acl_bad_lookup
		= 0;
	default_acl_local_appends = default_acl_unexpected_dependencies = 0;
	default_acl_expected_boundary = route + 1;
	default_acl_named_privilege = form >= 120;
	default_acl_target = form % 5;
	default_acl_grantee = (RoleSpec){ .type = T_RoleSpec,
									  .roletype = route == 0 ? ROLESPEC_CSTRING : ROLESPEC_PUBLIC,
									  .rolename = "default_grantee" };
	default_acl_owner = (RoleSpec){ .type = T_RoleSpec,
									.roletype = ROLESPEC_CSTRING,
									.rolename = "default_owner" };
	default_acl_grantee_cell = (ListCell){ .ptr_value = &default_acl_grantee };
	default_acl_grantees = (List){ .type = T_List,
								   .length = 1,
								   .max_length = 1,
								   .elements = &default_acl_grantee_cell };
	default_acl_role_cell = (ListCell){ .ptr_value = &default_acl_owner };
	default_acl_roles = (List){ .type = T_List,
								.length = 1,
								.max_length = 1,
								.elements = &default_acl_role_cell };
	default_acl_schema = (String){ .type = T_String, .sval = "default_schema" };
	default_acl_schema_cell = (ListCell){ .ptr_value = &default_acl_schema };
	default_acl_schemas = (List){ .type = T_List,
								  .length = 1,
								  .max_length = 1,
								  .elements = &default_acl_schema_cell };
	default_acl_role_option
		= (DefElem){ .type = T_DefElem, .defname = "roles", .arg = (Node *)&default_acl_roles };
	default_acl_schema_option
		= (DefElem){ .type = T_DefElem, .defname = "schemas", .arg = (Node *)&default_acl_schemas };
	default_acl_option_cells[0]
		= (ListCell){ .ptr_value
					  = route == 1 ? &default_acl_role_option : &default_acl_schema_option };
	default_acl_option_cells[1] = (ListCell){ .ptr_value = &default_acl_schema_option };
	default_acl_options = (List){ .type = T_List,
								  .length = (route == 1) + schema,
								  .max_length = 2,
								  .elements = default_acl_option_cells };
	default_acl_privilege
		= (AccessPriv){ .type = T_AccessPriv,
						.priv_name = (char *)default_acl_privilege_names[default_acl_target] };
	default_acl_privilege_cell = (ListCell){ .ptr_value = &default_acl_privilege };
	default_acl_privileges = (List){ .type = T_List,
									 .length = 1,
									 .max_length = 1,
									 .elements = &default_acl_privilege_cell };
	default_acl_action
		= (GrantStmt){ .type = T_GrantStmt,
					   .is_grant = action < 2,
					   .targtype = ACL_TARGET_DEFAULTS,
					   .objtype = default_acl_targets[default_acl_target],
					   .grantees = &default_acl_grantees,
					   .grant_option = action == 1 || action == 3,
					   .behavior = action == 3 ? DROP_CASCADE : DROP_RESTRICT,
					   .privileges = default_acl_named_privilege ? &default_acl_privileges : NIL };
	native_default_acl_statement
		= (AlterDefaultPrivilegesStmt){ .type = T_AlterDefaultPrivilegesStmt,
										.action = &default_acl_action,
										.options
										= default_acl_options.length ? &default_acl_options : NIL };
	default_acl_expected_statement = (Node *)&native_default_acl_statement;
}

static void
native_default_acl_entry(void)
{
	default_acl_local_appends = 0; /* One local list per native invocation. */
	ExecAlterDefaultPrivilegesStmt(NULL, &native_default_acl_statement);
}

UT_TEST(test_native_default_acl_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 240; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_default_acl(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_default_acl_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(default_acl_lookups, repeat + 1);
				UT_ASSERT_EQ(default_acl_lookup_kind,
							 default_acl_lookups ? default_acl_expected_boundary : 0);
				UT_ASSERT_EQ(default_acl_unbound, 0);
				UT_ASSERT_EQ(default_acl_bad_lookup + default_acl_unexpected_dependencies, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	default_acl_lookup_active = false;
}

UT_TEST(test_native_default_acl_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 240; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_default_acl(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(default_acl_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_default_acl_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(default_acl_lookups, 0);
			UT_ASSERT_EQ(default_acl_lookup_kind,
						 default_acl_lookups ? default_acl_expected_boundary : 0);
			UT_ASSERT_EQ(default_acl_unbound + default_acl_bad_lookup
							 + default_acl_unexpected_dependencies,
						 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	default_acl_lookup_active = false;
}

UT_TEST(test_native_default_acl_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 240; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_default_acl(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_default_acl_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, "ALTER DEFAULT PRIVILEGES") != NULL);
				UT_ASSERT_EQ(default_acl_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(default_acl_lookup_kind,
							 default_acl_lookups ? default_acl_expected_boundary : 0);
				UT_ASSERT_EQ(default_acl_unbound + default_acl_bad_lookup
								 + default_acl_unexpected_dependencies,
							 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	default_acl_lookup_active = false;
}

UT_TEST(test_native_default_acl_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 240; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_default_acl(true, form);
			create_sql_entry(default_acl_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_default_acl_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(default_acl_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(default_acl_lookup_kind,
						 default_acl_lookups ? default_acl_expected_boundary : 0);
			UT_ASSERT_EQ(default_acl_unbound + default_acl_bad_lookup
							 + default_acl_unexpected_dependencies,
						 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	default_acl_lookup_active = false;
}

UT_TEST(test_native_default_acl_keeps_original_option_errors)
{
	for (unsigned failure = 0; failure < 3; failure++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_default_acl(binds, 0);
			set_native_operator_mode(mode);
			default_acl_option_cells[0].ptr_value
				= failure == 0 ? &default_acl_schema_option : &default_acl_role_option;
			default_acl_option_cells[1] = default_acl_option_cells[0];
			default_acl_options.length = failure == 2 ? 1 : 2;
			if (failure == 2)
				default_acl_role_option.defname = "unknown_default_acl_option";
			native_default_acl_statement.options = &default_acl_options;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_default_acl_entry();
			UT_ASSERT_EQ(caught, 1);
			if (failure < 2) {
				UT_ASSERT_EQ(error_code, ERRCODE_SYNTAX_ERROR);
				UT_ASSERT_STR_EQ(error_message, "conflicting or redundant options");
			} else
				UT_ASSERT_STR_EQ(error_message,
								 "option \"unknown_default_acl_option\" not recognized");
			UT_ASSERT_EQ(default_acl_lookups + default_acl_local_appends, 0);
			UT_ASSERT_EQ(default_acl_unbound + default_acl_bad_lookup
							 + default_acl_unexpected_dependencies,
						 0);
			UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
		}
	}
	default_acl_lookup_active = false;
}

UT_TEST(test_native_default_acl_keeps_original_privilege_errors)
{
	for (unsigned failure = 0; failure < 4; failure++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_default_acl(binds,
									 160); /* PUBLIC, current role, TABLE, named privilege. */
			set_native_operator_mode(mode);
			if (failure == 0)
				default_acl_privilege.cols = &default_acl_schemas;
			else if (failure == 1)
				default_acl_privilege.priv_name = NULL;
			else
				default_acl_privilege.priv_name = failure == 2 ? "unknown_privilege" : "execute";
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_default_acl_entry();
			UT_ASSERT_EQ(caught, 1);
			if (failure == 0 || failure == 3)
				UT_ASSERT_EQ(error_code, ERRCODE_INVALID_GRANT_OPERATION);
			else if (failure == 2)
				UT_ASSERT_EQ(error_code, ERRCODE_SYNTAX_ERROR);
			UT_ASSERT_STR_EQ(error_message,
							 failure == 0	? "default privileges cannot be set for columns"
							 : failure == 1 ? "AccessPriv node must specify privilege"
							 : failure == 2 ? "unrecognized privilege type \"unknown_privilege\""
											: "invalid privilege type EXECUTE for relation");
			UT_ASSERT_EQ(default_acl_lookups, 0);
			UT_ASSERT_EQ(default_acl_local_appends, 1);
			UT_ASSERT_EQ(default_acl_unbound + default_acl_bad_lookup
							 + default_acl_unexpected_dependencies,
						 0);
			UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
		}
	}
	default_acl_lookup_active = false;
}

/* Stop before each original native rename callee, never return success. */
static ObjectAddress
observe_native_rename_stmt(RenameStmt *stmt, unsigned route)
{
	if (!rename_dispatch_lookup_active)
		siglongjmp(boundary, 3);
	rename_dispatch_lookups++;
	rename_dispatch_lookup_kind = route;
	if (rename_dispatch_expect_binding && utility_binds == 0)
		rename_dispatch_unbound++;
	if (stmt != &native_rename_dispatch_statement || route != rename_dispatch_expected_boundary)
		rename_dispatch_bad_lookup++;
	siglongjmp(boundary, 2); /* Before the original native callee. */
}

static ObjectAddress
observe_native_rename_names(const char *oldname, const char *newname, unsigned route)
{
	if (oldname != native_rename_dispatch_statement.subname
		|| newname != native_rename_dispatch_statement.newname)
		rename_dispatch_bad_lookup++;
	return observe_native_rename_stmt(&native_rename_dispatch_statement, route);
}

static ObjectAddress
observe_native_rename_rule(RangeVar *relation, const char *oldname, const char *newname)
{
	if (relation != native_rename_dispatch_statement.relation)
		rename_dispatch_bad_lookup++;
	return observe_native_rename_names(oldname, newname, 11);
}

static Relation
native_rename_unexpected_relation(void)
{
	rename_dispatch_unexpected_dependencies++;
	siglongjmp(boundary, 3);
}

static void
native_rename_unexpected_void(void)
{
	rename_dispatch_unexpected_dependencies++;
	siglongjmp(boundary, 3);
}

#define RenameConstraint(s) observe_native_rename_stmt((s), 1)
#define RenameRelation(s) observe_native_rename_stmt((s), 2)
#define renameatt(s) observe_native_rename_stmt((s), 3)
#define renametrig(s) observe_native_rename_stmt((s), 4)
#define rename_policy(s) observe_native_rename_stmt((s), 5)
#define RenameType(s) observe_native_rename_stmt((s), 6)
#define RenameDatabase(o, n) observe_native_rename_names((o), (n), 7)
#define RenameRole(o, n) observe_native_rename_names((o), (n), 8)
#define RenameSchema(o, n) observe_native_rename_names((o), (n), 9)
#define RenameTableSpace(o, n) observe_native_rename_names((o), (n), 10)
#define RenameRewriteRule(r, o, n) observe_native_rename_rule((r), (o), (n))
#define table_open(...) native_rename_unexpected_relation()
#define table_close(...) native_rename_unexpected_void()
#define AlterObjectRename_internal(...) native_rename_unexpected_void()
#include "test_cluster_fresh_rename_dispatch.inc"
#undef RenameConstraint
#undef RenameRelation
#undef renameatt
#undef renametrig
#undef rename_policy
#undef RenameType
#undef RenameDatabase
#undef RenameRole
#undef RenameSchema
#undef RenameTableSpace
#undef RenameRewriteRule
#undef table_open
#undef table_close
#undef AlterObjectRename_internal

static RangeVar rename_dispatch_relation;
static String rename_dispatch_name;
static ListCell rename_dispatch_name_cell;
static List rename_dispatch_names;
static ObjectWithArgs rename_dispatch_routine;
static const struct {
	ObjectType type;
	unsigned route;
	const char *command;
} rename_dispatch_targets[] = { { OBJECT_TABCONSTRAINT, 1, "ALTER TABLE" },
								{ OBJECT_DATABASE, 7, "ALTER DATABASE" },
								{ OBJECT_ROLE, 8, "ALTER ROLE" },
								{ OBJECT_SCHEMA, 9, "ALTER SCHEMA" },
								{ OBJECT_TABLESPACE, 10, "ALTER TABLESPACE" },
								{ OBJECT_TABLE, 2, "ALTER TABLE" },
								{ OBJECT_SEQUENCE, 2, "ALTER SEQUENCE" },
								{ OBJECT_VIEW, 2, "ALTER VIEW" },
								{ OBJECT_INDEX, 2, "ALTER INDEX" },
								{ OBJECT_FOREIGN_TABLE, 2, "ALTER FOREIGN TABLE" },
								{ OBJECT_COLUMN, 3, "ALTER TABLE" },
								{ OBJECT_RULE, 11, "ALTER RULE" },
								{ OBJECT_TRIGGER, 4, "ALTER TRIGGER" },
								{ OBJECT_POLICY, 5, "ALTER POLICY" },
								{ OBJECT_AGGREGATE, 12, "ALTER AGGREGATE" },
								{ OBJECT_COLLATION, 12, "ALTER COLLATION" },
								{ OBJECT_CONVERSION, 12, "ALTER CONVERSION" },
								{ OBJECT_EVENT_TRIGGER, 12, "ALTER EVENT TRIGGER" },
								{ OBJECT_FDW, 12, "ALTER FOREIGN DATA WRAPPER" },
								{ OBJECT_FOREIGN_SERVER, 12, "ALTER SERVER" },
								{ OBJECT_FUNCTION, 12, "ALTER FUNCTION" },
								{ OBJECT_LANGUAGE, 12, "ALTER LANGUAGE" },
								{ OBJECT_PROCEDURE, 12, "ALTER PROCEDURE" },
								{ OBJECT_ROUTINE, 12, "ALTER ROUTINE" },
								{ OBJECT_STATISTIC_EXT, 12, "ALTER STATISTICS" },
								{ OBJECT_TSCONFIGURATION, 12, "ALTER TEXT SEARCH CONFIGURATION" },
								{ OBJECT_TSDICTIONARY, 12, "ALTER TEXT SEARCH DICTIONARY" },
								{ OBJECT_TSPARSER, 12, "ALTER TEXT SEARCH PARSER" },
								{ OBJECT_TSTEMPLATE, 12, "ALTER TEXT SEARCH TEMPLATE" },
								{ OBJECT_PUBLICATION, 12, "ALTER PUBLICATION" },
								{ OBJECT_SUBSCRIPTION, 12, "ALTER SUBSCRIPTION" } };

static void
reset_native_rename_dispatch(bool expect_binding, unsigned form)
{
	ObjectType type = rename_dispatch_targets[form % 31].type;

	reset_create_preflight();
	rename_dispatch_lookup_active = true;
	rename_dispatch_expect_binding = expect_binding;
	rename_dispatch_lookups = rename_dispatch_lookup_kind = rename_dispatch_unbound
		= rename_dispatch_bad_lookup = 0;
	rename_dispatch_unexpected_dependencies = 0;
	rename_dispatch_expected_boundary = rename_dispatch_targets[form % 31].route;
	rename_dispatch_relation = (RangeVar){ .type = T_RangeVar, .relname = "rename_relation" };
	rename_dispatch_name = (String){ .type = T_String, .sval = "rename_object" };
	rename_dispatch_name_cell = (ListCell){ .ptr_value = &rename_dispatch_name };
	rename_dispatch_names = (List){ .type = T_List,
									.length = 1,
									.max_length = 1,
									.elements = &rename_dispatch_name_cell };
	rename_dispatch_routine
		= (ObjectWithArgs){ .type = T_ObjectWithArgs, .objname = &rename_dispatch_names };
	native_rename_dispatch_statement
		= (RenameStmt){ .type = T_RenameStmt,
						.renameType = type,
						.relationType = OBJECT_TABLE,
						.relation = &rename_dispatch_relation,
						.object = type == OBJECT_AGGREGATE || type == OBJECT_FUNCTION
										  || type == OBJECT_PROCEDURE || type == OBJECT_ROUTINE
									  ? (Node *)&rename_dispatch_routine
									  : (Node *)&rename_dispatch_names,
						.subname = "rename_source",
						.newname = "rename_destination",
						.missing_ok = form >= 31 };
	rename_dispatch_expected_statement = (Node *)&native_rename_dispatch_statement;
}

static void
native_rename_dispatch_entry(void)
{
	(void)ExecRenameStmt(&native_rename_dispatch_statement);
}

UT_TEST(test_native_rename_dispatch_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 62; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_rename_dispatch(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_rename_dispatch_entry();
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(rename_dispatch_lookups, repeat + 1);
				UT_ASSERT_EQ(rename_dispatch_lookup_kind,
							 rename_dispatch_lookups ? rename_dispatch_expected_boundary : 0);
				UT_ASSERT_EQ(rename_dispatch_unbound, 0);
				UT_ASSERT_EQ(rename_dispatch_bad_lookup + rename_dispatch_unexpected_dependencies,
							 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	rename_dispatch_lookup_active = false;
}

UT_TEST(test_native_rename_dispatch_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 62; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_rename_dispatch(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(rename_dispatch_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_rename_dispatch_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(rename_dispatch_lookups, 0);
			UT_ASSERT_EQ(rename_dispatch_lookup_kind,
						 rename_dispatch_lookups ? rename_dispatch_expected_boundary : 0);
			UT_ASSERT_EQ(rename_dispatch_unbound + rename_dispatch_bad_lookup
							 + rename_dispatch_unexpected_dependencies,
						 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	rename_dispatch_lookup_active = false;
}

UT_TEST(test_native_rename_dispatch_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 62; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_rename_dispatch(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_rename_dispatch_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, rename_dispatch_targets[form % 31].command)
							  != NULL);
				UT_ASSERT_EQ(rename_dispatch_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(rename_dispatch_lookup_kind,
							 rename_dispatch_lookups ? rename_dispatch_expected_boundary : 0);
				UT_ASSERT_EQ(rename_dispatch_unbound + rename_dispatch_bad_lookup
								 + rename_dispatch_unexpected_dependencies,
							 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	rename_dispatch_lookup_active = false;
}

UT_TEST(test_native_rename_dispatch_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 62; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_rename_dispatch(true, form);
			create_sql_entry(rename_dispatch_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_rename_dispatch_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(rename_dispatch_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(rename_dispatch_lookup_kind,
						 rename_dispatch_lookups ? rename_dispatch_expected_boundary : 0);
			UT_ASSERT_EQ(rename_dispatch_unbound + rename_dispatch_bad_lookup
							 + rename_dispatch_unexpected_dependencies,
						 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	rename_dispatch_lookup_active = false;
}

UT_TEST(test_native_rename_dispatch_scope_refusals_match_utility_before_binding)
{
	static const struct {
		ObjectType type, relation_type;
		const char *message;
	} forbidden[]
		= { { OBJECT_MATVIEW, OBJECT_TABLE, "materialized views is not supported in shared mode" },
			{ OBJECT_TYPE, OBJECT_TABLE, "custom types is not supported in shared mode" },
			{ OBJECT_DOMAIN, OBJECT_TABLE, "custom types is not supported in shared mode" },
			{ OBJECT_OPERATOR, OBJECT_TABLE, "custom operators is not supported in shared mode" },
			{ OBJECT_OPCLASS, OBJECT_TABLE, "custom operators is not supported in shared mode" },
			{ OBJECT_OPFAMILY, OBJECT_TABLE, "custom operators is not supported in shared mode" },
			{ OBJECT_EXTENSION, OBJECT_TABLE,
			  "CREATE/DROP EXTENSION is not supported in shared mode" },
			{ OBJECT_COLUMN, OBJECT_TYPE, "custom types is not supported in shared mode" },
			{ OBJECT_ATTRIBUTE, OBJECT_TYPE, "custom types is not supported in shared mode" },
			{ OBJECT_DOMCONSTRAINT, OBJECT_TABLE, "custom types is not supported in shared mode" },
			{ OBJECT_COLUMN, OBJECT_MATVIEW, "materialized views is not supported in shared mode" },
			{ OBJECT_TYPE, OBJECT_MATVIEW, "custom types is not supported in shared mode" },
			{ OBJECT_MATVIEW, OBJECT_TYPE, "custom types is not supported in shared mode" } };

	for (unsigned type = 0; type < lengthof(forbidden); type++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			int caught;

			if (mode == 0 || mode == 2)
				continue; /* Unsupported policy is scoped to original config OR catalog. */
			reset_native_rename_dispatch(false, 0);
			set_native_operator_mode(mode);
			native_rename_dispatch_statement.renameType = forbidden[type].type;
			native_rename_dispatch_statement.relationType = forbidden[type].relation_type;
			XactReadOnly = utility_parallel = utility_recovery = utility_in_child = true;
			utility_bind_ok = false;
			for (unsigned native = 0; native < 2; native++) {
				reset_error();
				caught = sigsetjmp(boundary, 1);
				if (!caught) {
					if (native)
						native_rename_dispatch_entry();
					else
						create_sql_entry(rename_dispatch_expected_statement);
				}
				UT_ASSERT_EQ(caught, 1);
				UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
				UT_ASSERT_STR_EQ(error_message, forbidden[type].message);
				UT_ASSERT_STR_EQ(error_detail,
								 "PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
				UT_ASSERT_EQ(rename_dispatch_lookups + rename_dispatch_unbound
								 + rename_dispatch_bad_lookup
								 + rename_dispatch_unexpected_dependencies,
							 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	rename_dispatch_lookup_active = false;
}

UT_TEST(test_native_rename_dispatch_keeps_original_unrecognized_target_error)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool binds = mode == 3 || mode == 7;
		char expected[100];
		int caught;

		reset_native_rename_dispatch(binds, 0);
		set_native_operator_mode(mode);
		native_rename_dispatch_statement.renameType = OBJECT_LARGEOBJECT;
		snprintf(expected, sizeof(expected), "unrecognized rename stmt type: %d",
				 (int)OBJECT_LARGEOBJECT);
		caught = sigsetjmp(boundary, 1);
		if (!caught)
			native_rename_dispatch_entry();
		UT_ASSERT_EQ(caught, 1);
		UT_ASSERT_STR_EQ(error_message, expected);
		UT_ASSERT_EQ(rename_dispatch_lookups + rename_dispatch_unbound + rename_dispatch_bad_lookup
						 + rename_dispatch_unexpected_dependencies,
					 0);
		UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
		UT_ASSERT_EQ(utility_prepares, binds ? 1 : 0);
		UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
	}
	rename_dispatch_lookup_active = false;
}

/* Observe before the original namespace callee, never read its local oldOid. */
static ObjectAddress
observe_native_schema_dispatch(unsigned route, Oid *oldschema)
{
	if (!schema_dispatch_lookup_active)
		siglongjmp(boundary, 3);
	schema_dispatch_lookups++;
	schema_dispatch_lookup_kind = route;
	if (schema_dispatch_expect_binding && utility_binds == 0)
		schema_dispatch_unbound++;
	if (route != schema_dispatch_expected_boundary
		|| (oldschema != NULL) != schema_dispatch_wants_old_schema)
		schema_dispatch_bad_lookup++;
	siglongjmp(boundary, 2); /* Before the real callee/query; no output is written. */
}

static ObjectAddress
observe_native_schema_table(AlterObjectSchemaStmt *stmt, Oid *oldschema)
{
	if (stmt != &native_schema_dispatch_statement)
		schema_dispatch_bad_lookup++;
	return observe_native_schema_dispatch(1, oldschema);
}

static ObjectAddress
observe_native_schema_type(List *names, const char *newschema, ObjectType type, Oid *oldschema)
{
	if (names != (List *)native_schema_dispatch_statement.object
		|| newschema != native_schema_dispatch_statement.newschema
		|| type != native_schema_dispatch_statement.objectType)
		schema_dispatch_bad_lookup++;
	return observe_native_schema_dispatch(2, oldschema);
}

static ObjectAddress
observe_native_schema_extension(const char *name, const char *newschema, Oid *oldschema)
{
	if (name != strVal(native_schema_dispatch_statement.object)
		|| newschema != native_schema_dispatch_statement.newschema)
		schema_dispatch_bad_lookup++;
	return observe_native_schema_dispatch(3, oldschema);
}

static Relation
native_schema_dispatch_unexpected_relation(void)
{
	schema_dispatch_unexpected_dependencies++;
	siglongjmp(boundary, 3);
}

static Oid
native_schema_dispatch_unexpected_oid(void)
{
	schema_dispatch_unexpected_dependencies++;
	siglongjmp(boundary, 3);
}

static void
native_schema_dispatch_unexpected_void(void)
{
	schema_dispatch_unexpected_dependencies++;
	siglongjmp(boundary, 3);
}

#define AlterTableNamespace(s, o) observe_native_schema_table((s), (o))
#define AlterTypeNamespace(n, s, t, o) observe_native_schema_type((n), (s), (t), (o))
#define AlterExtensionNamespace(n, s, o) observe_native_schema_extension((n), (s), (o))
#define table_open(...) native_schema_dispatch_unexpected_relation()
#define table_close(...) native_schema_dispatch_unexpected_void()
#define LookupCreationNamespace(...) native_schema_dispatch_unexpected_oid()
#define AlterObjectNamespace_internal(...) native_schema_dispatch_unexpected_oid()
#include "test_cluster_fresh_schema_dispatch.inc"
#undef AlterTableNamespace
#undef AlterTypeNamespace
#undef AlterExtensionNamespace
#undef table_open
#undef table_close
#undef LookupCreationNamespace
#undef AlterObjectNamespace_internal

static RangeVar schema_dispatch_relation;
static String schema_dispatch_name;
static ListCell schema_dispatch_name_cell;
static List schema_dispatch_names;
static ObjectWithArgs schema_dispatch_routine;
static const struct {
	ObjectType type;
	unsigned route;
	const char *command;
} schema_dispatch_targets[] = { { OBJECT_TABLE, 1, "ALTER TABLE" },
								{ OBJECT_FOREIGN_TABLE, 1, "ALTER FOREIGN TABLE" },
								{ OBJECT_SEQUENCE, 1, "ALTER SEQUENCE" },
								{ OBJECT_VIEW, 1, "ALTER VIEW" },
								{ OBJECT_AGGREGATE, 4, "ALTER AGGREGATE" },
								{ OBJECT_COLLATION, 4, "ALTER COLLATION" },
								{ OBJECT_CONVERSION, 4, "ALTER CONVERSION" },
								{ OBJECT_FUNCTION, 4, "ALTER FUNCTION" },
								{ OBJECT_PROCEDURE, 4, "ALTER PROCEDURE" },
								{ OBJECT_ROUTINE, 4, "ALTER ROUTINE" },
								{ OBJECT_STATISTIC_EXT, 4, "ALTER STATISTICS" },
								{ OBJECT_TSCONFIGURATION, 4, "ALTER TEXT SEARCH CONFIGURATION" },
								{ OBJECT_TSDICTIONARY, 4, "ALTER TEXT SEARCH DICTIONARY" },
								{ OBJECT_TSPARSER, 4, "ALTER TEXT SEARCH PARSER" },
								{ OBJECT_TSTEMPLATE, 4, "ALTER TEXT SEARCH TEMPLATE" } };

static void
reset_native_schema_dispatch(bool expect_binding, unsigned form)
{
	ObjectType type = schema_dispatch_targets[form % 15].type;

	reset_create_preflight();
	schema_dispatch_lookup_active = true;
	schema_dispatch_expect_binding = expect_binding;
	schema_dispatch_lookups = schema_dispatch_lookup_kind = schema_dispatch_unbound
		= schema_dispatch_bad_lookup = 0;
	schema_dispatch_unexpected_dependencies = 0;
	schema_dispatch_expected_boundary = schema_dispatch_targets[form % 15].route;
	schema_dispatch_wants_old_schema = (form / 15) % 2;
	schema_dispatch_old_schema_address
		= (ObjectAddress){ .classId = 901, .objectId = 902, .objectSubId = 903 };
	schema_dispatch_relation = (RangeVar){ .type = T_RangeVar, .relname = "schema_relation" };
	schema_dispatch_name = (String){ .type = T_String, .sval = "schema_source_object" };
	schema_dispatch_name_cell = (ListCell){ .ptr_value = &schema_dispatch_name };
	schema_dispatch_names = (List){ .type = T_List,
									.length = 1,
									.max_length = 1,
									.elements = &schema_dispatch_name_cell };
	schema_dispatch_routine
		= (ObjectWithArgs){ .type = T_ObjectWithArgs, .objname = &schema_dispatch_names };
	native_schema_dispatch_statement
		= (AlterObjectSchemaStmt){ .type = T_AlterObjectSchemaStmt,
								   .objectType = type,
								   .relation = &schema_dispatch_relation,
								   .object = type == OBJECT_AGGREGATE || type == OBJECT_FUNCTION
													 || type == OBJECT_PROCEDURE
													 || type == OBJECT_ROUTINE
												 ? (Node *)&schema_dispatch_routine
												 : (Node *)&schema_dispatch_names,
								   .newschema = "schema_destination",
								   .missing_ok = form >= 30 };
	schema_dispatch_expected_statement = (Node *)&native_schema_dispatch_statement;
}

static void
assert_native_schema_dispatch_output_untouched(void)
{
	UT_ASSERT_EQ(schema_dispatch_old_schema_address.classId, 901);
	UT_ASSERT_EQ(schema_dispatch_old_schema_address.objectId, 902);
	UT_ASSERT_EQ(schema_dispatch_old_schema_address.objectSubId, 903);
}

static void
native_schema_dispatch_entry(void)
{
	(void)ExecAlterObjectSchemaStmt(
		&native_schema_dispatch_statement,
		schema_dispatch_wants_old_schema ? &schema_dispatch_old_schema_address : NULL);
}

UT_TEST(test_native_schema_dispatch_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 60; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_schema_dispatch(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_schema_dispatch_entry();
				UT_ASSERT_EQ(caught, 2);
				assert_native_schema_dispatch_output_untouched();
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(schema_dispatch_lookups, repeat + 1);
				UT_ASSERT_EQ(schema_dispatch_lookup_kind,
							 schema_dispatch_lookups ? schema_dispatch_expected_boundary : 0);
				UT_ASSERT_EQ(schema_dispatch_unbound, 0);
				UT_ASSERT_EQ(schema_dispatch_bad_lookup + schema_dispatch_unexpected_dependencies,
							 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	schema_dispatch_lookup_active = false;
}

UT_TEST(test_native_schema_dispatch_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 60; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_schema_dispatch(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(schema_dispatch_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_schema_dispatch_entry();
			UT_ASSERT_EQ(caught, 1);
			assert_native_schema_dispatch_output_untouched();
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(schema_dispatch_lookups, 0);
			UT_ASSERT_EQ(schema_dispatch_lookup_kind,
						 schema_dispatch_lookups ? schema_dispatch_expected_boundary : 0);
			UT_ASSERT_EQ(schema_dispatch_unbound + schema_dispatch_bad_lookup
							 + schema_dispatch_unexpected_dependencies,
						 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	schema_dispatch_lookup_active = false;
}

UT_TEST(test_native_schema_dispatch_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 60; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_schema_dispatch(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_schema_dispatch_entry();
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				assert_native_schema_dispatch_output_untouched();
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, schema_dispatch_targets[form % 15].command)
							  != NULL);
				UT_ASSERT_EQ(schema_dispatch_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(schema_dispatch_lookup_kind,
							 schema_dispatch_lookups ? schema_dispatch_expected_boundary : 0);
				UT_ASSERT_EQ(schema_dispatch_unbound + schema_dispatch_bad_lookup
								 + schema_dispatch_unexpected_dependencies,
							 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	schema_dispatch_lookup_active = false;
}

UT_TEST(test_native_schema_dispatch_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 60; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_schema_dispatch(true, form);
			create_sql_entry(schema_dispatch_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_schema_dispatch_entry();
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			assert_native_schema_dispatch_output_untouched();
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(schema_dispatch_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(schema_dispatch_lookup_kind,
						 schema_dispatch_lookups ? schema_dispatch_expected_boundary : 0);
			UT_ASSERT_EQ(schema_dispatch_unbound + schema_dispatch_bad_lookup
							 + schema_dispatch_unexpected_dependencies,
						 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	schema_dispatch_lookup_active = false;
}

UT_TEST(test_native_schema_dispatch_scope_refusals_match_utility_before_binding)
{
	static const struct {
		ObjectType type;
		unsigned route;
		const char *message;
	} forbidden[]
		= { { OBJECT_MATVIEW, 1, "materialized views is not supported in shared mode" },
			{ OBJECT_TYPE, 2, "custom types is not supported in shared mode" },
			{ OBJECT_DOMAIN, 2, "custom types is not supported in shared mode" },
			{ OBJECT_OPERATOR, 4, "custom operators is not supported in shared mode" },
			{ OBJECT_OPCLASS, 4, "custom operators is not supported in shared mode" },
			{ OBJECT_OPFAMILY, 4, "custom operators is not supported in shared mode" },
			{ OBJECT_EXTENSION, 3, "CREATE/DROP EXTENSION is not supported in shared mode" } };

	for (unsigned type = 0; type < lengthof(forbidden); type++) {
		for (unsigned output = 0; output < 2; output++) {
			for (unsigned mode = 0; mode < 8; mode++) {
				bool shared = mode != 0 && mode != 2;
				int caught;

				reset_native_schema_dispatch(false, output * 15);
				set_native_operator_mode(mode);
				native_schema_dispatch_statement.objectType = forbidden[type].type;
				native_schema_dispatch_statement.object = forbidden[type].type == OBJECT_EXTENSION
															  ? (Node *)&schema_dispatch_name
															  : (Node *)&schema_dispatch_names;
				schema_dispatch_expected_boundary = forbidden[type].route;
				XactReadOnly = utility_parallel = utility_recovery = utility_in_child = true;
				utility_bind_ok = false;
				if (shared) {
					caught = sigsetjmp(boundary, 1);
					if (!caught)
						create_sql_entry(schema_dispatch_expected_statement);
					UT_ASSERT_EQ(caught, 1);
					UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
					UT_ASSERT_STR_EQ(error_message, forbidden[type].message);
					UT_ASSERT_STR_EQ(
						error_detail,
						"PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
					UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
					assert_native_schema_dispatch_output_untouched();
					reset_error();
				}
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_schema_dispatch_entry();
				UT_ASSERT_EQ(caught, shared ? 1 : 2);
				UT_ASSERT_EQ(error_code, shared ? ERRCODE_FEATURE_NOT_SUPPORTED : 0);
				if (shared) {
					UT_ASSERT_STR_EQ(error_message, forbidden[type].message);
					UT_ASSERT_STR_EQ(
						error_detail,
						"PGRAC_FAMILY=SHARED_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED");
				}
				UT_ASSERT_EQ(schema_dispatch_lookups, shared ? 0 : 1);
				UT_ASSERT_EQ(schema_dispatch_lookup_kind,
							 shared ? 0 : schema_dispatch_expected_boundary);
				UT_ASSERT_EQ(schema_dispatch_unbound + schema_dispatch_bad_lookup
								 + schema_dispatch_unexpected_dependencies,
							 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
				assert_native_schema_dispatch_output_untouched();
			}
		}
	}
	schema_dispatch_lookup_active = false;
}

UT_TEST(test_native_schema_dispatch_keeps_original_unrecognized_target_error)
{
	for (unsigned output = 0; output < 2; output++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			char expected[100];
			int caught;

			reset_native_schema_dispatch(binds, output * 15);
			set_native_operator_mode(mode);
			native_schema_dispatch_statement.objectType = OBJECT_LARGEOBJECT;
			snprintf(expected, sizeof(expected), "unrecognized AlterObjectSchemaStmt type: %d",
					 (int)OBJECT_LARGEOBJECT);
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_schema_dispatch_entry();
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_STR_EQ(error_message, expected);
			UT_ASSERT_EQ(schema_dispatch_lookups + schema_dispatch_unbound
							 + schema_dispatch_bad_lookup + schema_dispatch_unexpected_dependencies,
						 0);
			UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, binds ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
			assert_native_schema_dispatch_output_untouched();
		}
	}
	schema_dispatch_lookup_active = false;
}

/* Actual database identity prefixes stop at the first pg_database lock.
 * Resolution, parameters, permissions, ACL and mutation remain beyond this boundary. */
#include "test_cluster_fresh_database_identity.inc"
static RenameStmt native_database_identity_rename;
static AlterOwnerStmt native_database_identity_owner;
static String database_identity_name;
static RoleSpec database_identity_role;
static Node *database_identity_expected_statement;
static const char *database_identity_oldname, *database_identity_newname;
static Oid database_identity_newowner;

static void
reset_native_database_identity(bool expect_binding, unsigned form)
{
	static const char *const oldnames[] = { "identity_database", "other_identity_database",
											"identity_database", "other_identity_database" };
	static const char *const newnames[]
		= { "renamed_identity_database", "second_renamed_database" };
	static const Oid owners[] = { 901, 902 };

	reset_create_preflight();
	database_identity_lookup_active = true;
	database_identity_expect_binding = expect_binding;
	database_identity_lookups = database_identity_unbound = database_identity_bad_lookup = 0;
	database_identity_oldname = oldnames[form];
	database_identity_newname = form < 2 ? newnames[form] : NULL;
	database_identity_newowner = form < 2 ? InvalidOid : owners[form - 2];
	database_identity_name
		= (String){ .type = T_String, .sval = (char *)database_identity_oldname };
	database_identity_role = (RoleSpec){ .type = T_RoleSpec,
										 .roletype = ROLESPEC_CSTRING,
										 .rolename = "identity_new_owner",
										 .location = -1 };
	native_database_identity_rename = (RenameStmt){ .type = T_RenameStmt,
													.renameType = OBJECT_DATABASE,
													.subname = (char *)database_identity_oldname,
													.newname = (char *)database_identity_newname };
	native_database_identity_owner = (AlterOwnerStmt){ .type = T_AlterOwnerStmt,
													   .objectType = OBJECT_DATABASE,
													   .object = (Node *)&database_identity_name,
													   .newowner = &database_identity_role };
	database_identity_expected_statement = form < 2 ? (Node *)&native_database_identity_rename
													: (Node *)&native_database_identity_owner;
}

static void
native_database_identity_entry(unsigned form)
{
	if (form < 2)
		(void)RenameDatabase(database_identity_oldname, database_identity_newname);
	else
		(void)AlterDatabaseOwner(database_identity_oldname, database_identity_newowner);
}

UT_TEST(test_native_database_identity_binds_before_first_query_in_all_profiles)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;
			int caught;

			reset_native_database_identity(binds, form);
			set_native_operator_mode(mode);
			for (unsigned repeat = 0; repeat < 2; repeat++) {
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_database_identity_entry(form);
				UT_ASSERT_EQ(caught, 2);
				UT_ASSERT_EQ(error_code, 0);
				UT_ASSERT_EQ(database_identity_lookups, repeat + 1);
				UT_ASSERT_EQ(database_identity_unbound, 0);
				UT_ASSERT_EQ(database_identity_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_prepares, binds ? repeat + 1 : 0);
				UT_ASSERT_EQ(utility_binds, binds ? 1 : 0);
				UT_ASSERT_EQ(utility_top_xid, binds ? 400 : InvalidTransactionId);
			}
		}
	}
	database_identity_lookup_active = false;
}

UT_TEST(test_native_database_identity_refuses_before_lookup_on_publication_child_and_top_errors)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned failure = 0; failure < 5; failure++) {
			bool allocated = failure == 0 || failure == 2;
			int caught;

			reset_native_database_identity(true, form);
			if (failure == 0)
				utility_bind_ok = false;
			else if (failure == 1 || failure == 2) {
				if (failure == 2)
					create_sql_entry(database_identity_expected_statement);
				utility_in_child = true;
			} else {
				utility_current_xid = 401;
				utility_top_xid = failure == 3 ? 400 : InvalidTransactionId;
			}
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_database_identity_entry(form);
			UT_ASSERT_EQ(caught, 1);
			UT_ASSERT_EQ(error_code, failure == 0  ? ERRCODE_DATA_CORRUPTED
									 : failure < 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
												   : ERRCODE_INTERNAL_ERROR);
			UT_ASSERT_STR_EQ(
				error_message,
				failure == 0
					? "canonical ACTIVE publication failed before shared catalog alteration"
				: failure < 3 ? "shared catalog alteration does not support subtransactions"
							  : "shared catalog top-level transaction identity mismatch");
			UT_ASSERT_EQ(database_identity_lookups, 0);
			UT_ASSERT_EQ(database_identity_unbound + database_identity_bad_lookup, 0);
			UT_ASSERT_EQ(utility_xids, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_prepares, allocated ? 1 : 0);
			UT_ASSERT_EQ(utility_binds, failure == 2 ? 1 : 0);
			UT_ASSERT_EQ(utility_top_xid, allocated || failure == 3 ? 400 : InvalidTransactionId);
		}
	}
	database_identity_lookup_active = false;
}

UT_TEST(test_native_database_identity_context_guards_keep_nonshared_route)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool binds = mode == 3 || mode == 7;

			for (unsigned guard = 0; guard < 3; guard++) {
				int caught;

				reset_native_database_identity(binds, form);
				set_native_operator_mode(mode);
				XactReadOnly = guard == 0;
				utility_parallel = guard == 1;
				utility_recovery = guard == 2;
				caught = sigsetjmp(boundary, 1);
				if (!caught)
					native_database_identity_entry(form);
				UT_ASSERT_EQ(caught, binds ? 1 : 2);
				UT_ASSERT_EQ(error_code, !binds		  ? 0
										 : guard == 1 ? ERRCODE_INVALID_TRANSACTION_STATE
													  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
				if (binds)
					UT_ASSERT(strstr(error_message, "ALTER DATABASE") != NULL);
				UT_ASSERT_EQ(database_identity_lookups, binds ? 0 : 1);
				UT_ASSERT_EQ(database_identity_unbound + database_identity_bad_lookup, 0);
				UT_ASSERT_EQ(utility_xids + utility_prepares + utility_binds, 0);
			}
		}
	}
	database_identity_lookup_active = false;
}

UT_TEST(test_native_database_identity_utility_warm_guards_preserve_binding)
{
	for (unsigned form = 0; form < 4; form++) {
		for (unsigned guard = 0; guard < 4; guard++) {
			int caught;

			reset_native_database_identity(true, form);
			create_sql_entry(database_identity_expected_statement);
			UT_ASSERT_EQ(utility_prepares, 1);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			XactReadOnly = guard == 1;
			utility_parallel = guard == 2;
			utility_recovery = guard == 3;
			caught = sigsetjmp(boundary, 1);
			if (!caught)
				native_database_identity_entry(form);
			UT_ASSERT_EQ(caught, guard ? 1 : 2);
			UT_ASSERT_EQ(error_code, !guard		  ? 0
									 : guard == 2 ? ERRCODE_INVALID_TRANSACTION_STATE
												  : ERRCODE_READ_ONLY_SQL_TRANSACTION);
			UT_ASSERT_EQ(database_identity_lookups, guard ? 0 : 1);
			UT_ASSERT_EQ(database_identity_unbound + database_identity_bad_lookup, 0);
			UT_ASSERT_EQ(utility_prepares, guard ? 1 : 2);
			UT_ASSERT_EQ(utility_xids, 1);
			UT_ASSERT_EQ(utility_binds, 1);
			UT_ASSERT_EQ(utility_top_xid, 400);
		}
	}
	database_identity_lookup_active = false;
}

int
main(void)
{
	UT_PLAN(249);
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
	UT_RUN(test_materialized_view_dependency_refuses_before_binding);
	UT_RUN(test_foreign_table_unsupported_base_refuses_before_binding);
	UT_RUN(test_undo_tablespace_owner_refuses_before_binding);
	UT_RUN(test_undo_tablespace_options_refuses_before_binding);
	UT_RUN(test_database_move_keeps_native_path_without_binding);
	UT_RUN(test_unsupported_security_labels_refuse_before_binding);
	UT_RUN(test_alter_rewrite_candidates_keep_native_path_without_binding);
	UT_RUN(test_unsupported_alter_table_refuses_before_binding);
	UT_RUN(test_drop_owned_autocommit_binds_before_later_work_and_reuses_top);
	UT_RUN(test_drop_owned_autocommit_refusal_child_and_wrong_top_precede_later_work);
	UT_RUN(test_drop_owned_autocommit_readonly_parallel_precede_binding);
	UT_RUN(test_drop_owned_locked_storage_precheck_keeps_original_path_without_binding);
	UT_RUN(test_drop_autocommit_binds_before_later_work_and_reuses_top);
	UT_RUN(test_drop_autocommit_refusal_and_wrong_top_precede_later_work);
	UT_RUN(test_drop_autocommit_readonly_parallel_precede_binding);
	UT_RUN(test_drop_locked_or_concurrent_paths_keep_original_checks_without_binding);
	UT_RUN(test_drop_unsupported_objects_refuse_before_binding);
	UT_RUN(test_native_import_binds_before_lookup_and_callback_in_all_profiles);
	UT_RUN(test_native_import_refusal_and_invalid_scope_precede_catalogs);
	UT_RUN(test_native_import_original_readonly_parallel_recovery_guards_precede_binding);
	UT_RUN(test_native_import_original_errors_and_empty_callback_keep_top_binding);
	UT_RUN(test_native_import_filters_and_child_utility_reuse_original_top);
	UT_RUN(test_native_extension_binds_before_catalog_lock_in_all_profiles);
	UT_RUN(test_native_extension_refusal_child_and_wrong_top_precede_catalog_lock);
	UT_RUN(test_native_extension_original_readonly_parallel_recovery_guards_precede_binding);
	UT_RUN(test_native_extension_nested_and_forbidden_objects_refuse_before_binding);
	UT_RUN(test_native_extension_utility_prebinding_reuses_same_top_before_lookup);
	UT_RUN(test_native_unsupported_extension_commands_refuse_in_original_shared_profiles);
	UT_RUN(test_native_unsupported_extension_refusal_precedes_invalid_context_and_allocation);
	UT_RUN(test_nonshared_extension_create_retains_native_validation_skip_nested_and_options);
	UT_RUN(test_utility_and_native_unsupported_extension_code_message_detail_match);
	UT_RUN(test_native_schema_changes_bind_before_catalog_lock_in_all_profiles);
	UT_RUN(test_native_schema_refusal_child_or_wrong_top_precedes_catalog_lock);
	UT_RUN(test_native_schema_context_guards_precede_binding_and_preserve_nonshared_route);
	UT_RUN(test_native_schema_utility_prebinding_reuses_same_top_before_lookup);
	UT_RUN(test_native_comment_binds_before_database_and_object_lookup_in_all_profiles);
	UT_RUN(test_native_comment_refusal_child_and_wrong_top_precede_all_lookups);
	UT_RUN(test_native_comment_context_guards_keep_nonshared_native_route);
	UT_RUN(test_native_comment_utility_warm_binding_and_context_rejection_preserve_top);
	UT_RUN(test_native_comment_missing_database_retains_early_return_and_binding_cost);
	UT_RUN(test_native_security_labels_bind_before_object_lock_in_all_profiles);
	UT_RUN(test_native_security_label_refusals_precede_object_lock);
	UT_RUN(test_native_security_label_context_guards_keep_nonshared_route);
	UT_RUN(test_native_security_label_original_provider_and_type_errors_precede_allocation);
	UT_RUN(test_native_security_label_utility_warm_context_guards_preserve_binding);
	UT_RUN(test_native_security_label_original_shared_scope_matches_utility_refusal);
	UT_RUN(test_native_security_label_nonshared_object_type_routes_unchanged);
	UT_RUN(test_native_policy_changes_bind_before_role_setup_and_table_lock_in_all_profiles);
	UT_RUN(test_native_policy_refusals_precede_role_setup_and_table_lock);
	UT_RUN(test_native_policy_context_guards_keep_nonshared_route);
	UT_RUN(test_native_policy_utility_warm_context_guards_preserve_binding);
	UT_RUN(test_native_policy_create_retains_syntax_errors_before_allocation_and_valid_commands);
	UT_RUN(test_native_conversion_binds_before_namespace_lookup_in_all_profiles);
	UT_RUN(test_native_conversion_refusals_precede_namespace_lookup);
	UT_RUN(test_native_conversion_context_guards_keep_nonshared_route);
	UT_RUN(test_native_conversion_utility_warm_context_guards_preserve_binding);
	UT_RUN(test_native_collations_bind_before_namespace_lookup_or_catalog_lock_in_all_profiles);
	UT_RUN(test_native_collation_refusals_precede_lookup_or_lock);
	UT_RUN(test_native_collation_context_guards_keep_nonshared_route);
	UT_RUN(test_native_collation_utility_warm_context_guards_preserve_binding);
	UT_RUN(test_native_event_triggers_bind_before_role_cache_or_catalog_lock_in_all_profiles);
	UT_RUN(test_native_event_trigger_refusals_precede_role_cache_or_lock);
	UT_RUN(test_native_event_trigger_context_guards_keep_nonshared_route);
	UT_RUN(test_native_event_trigger_utility_warm_context_guards_preserve_binding);
	UT_RUN(test_native_event_trigger_original_permission_refusal_survives_early_binding);
	UT_RUN(test_native_owner_dispatch_binds_before_role_lookup_in_all_profiles);
	UT_RUN(test_native_owner_dispatch_publication_child_and_wrong_top_precede_role_lookup);
	UT_RUN(test_native_owner_dispatch_context_guards_keep_original_names_and_nonshared_route);
	UT_RUN(test_native_owner_dispatch_utility_warm_and_context_guards_preserve_binding);
	UT_RUN(test_native_owner_dispatch_original_type_refusal_matches_utility_scope_and_precedence);
	UT_RUN(test_native_owner_dispatch_pg_undo_name_refusal_matches_utility_before_binding);
	UT_RUN(test_native_language_binds_before_superuser_query_in_all_profiles);
	UT_RUN(test_native_language_publication_child_and_wrong_top_precede_superuser_query);
	UT_RUN(test_native_language_context_guards_keep_nonshared_route);
	UT_RUN(test_native_language_utility_warm_context_guards_preserve_binding);
	UT_RUN(test_native_language_original_permission_refusal_survives_early_binding);
	UT_RUN(test_native_access_method_binds_before_catalog_lock_in_all_profiles);
	UT_RUN(test_native_access_method_publication_child_and_wrong_top_precede_catalog_lock);
	UT_RUN(test_native_access_method_context_guards_keep_nonshared_route);
	UT_RUN(test_native_access_method_utility_warm_context_guards_preserve_binding);
	UT_RUN(test_native_function_binds_before_catalog_lock_in_all_profiles);
	UT_RUN(test_native_function_publication_child_and_wrong_top_precede_catalog_lock);
	UT_RUN(test_native_function_context_guards_keep_nonshared_route);
	UT_RUN(test_native_function_utility_warm_context_guards_preserve_binding);
	UT_RUN(test_native_fdw_binds_before_catalog_lock_in_all_profiles);
	UT_RUN(test_native_fdw_publication_child_and_wrong_top_precede_catalog_lock);
	UT_RUN(test_native_fdw_context_guards_keep_nonshared_route);
	UT_RUN(test_native_fdw_utility_warm_context_guards_preserve_binding);
	UT_RUN(test_native_foreign_server_binds_before_catalog_lock_in_all_profiles);
	UT_RUN(test_native_foreign_server_publication_child_and_wrong_top_precede_catalog_lock);
	UT_RUN(test_native_foreign_server_context_guards_keep_nonshared_route);
	UT_RUN(test_native_foreign_server_utility_warm_context_guards_preserve_binding);
	UT_RUN(test_native_user_mapping_binds_before_catalog_access_in_all_profiles);
	UT_RUN(test_native_user_mapping_publication_child_and_wrong_top_precede_catalog_access);
	UT_RUN(test_native_user_mapping_context_guards_keep_nonshared_route);
	UT_RUN(test_native_user_mapping_utility_warm_context_guards_preserve_binding);
	UT_RUN(test_native_statistics_alter_binds_before_lookup_in_all_profiles);
	UT_RUN(test_native_statistics_alter_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_statistics_alter_context_guards_keep_nonshared_route);
	UT_RUN(test_native_statistics_alter_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_statistics_alter_target_error_keeps_early_binding);
	UT_RUN(test_native_operator_scope_matches_utility_before_lookup_or_binding);
	UT_RUN(test_native_operator_scope_refusal_precedes_context_child_top_and_publication);
	UT_RUN(test_native_operator_scope_refusal_preserves_existing_top_binding);
	UT_RUN(test_native_opclass_scope_matches_utility_before_lookup_or_binding);
	UT_RUN(test_native_opclass_scope_refusal_precedes_context_child_top_and_publication);
	UT_RUN(test_native_opclass_scope_refusal_preserves_existing_top_binding);
	UT_RUN(test_native_aggregate_binds_before_namespace_query_in_all_profiles);
	UT_RUN(test_native_aggregate_refuses_before_query_on_publication_child_and_top_errors);
	UT_RUN(test_native_aggregate_context_guards_keep_nonshared_route);
	UT_RUN(test_native_aggregate_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_tsearch_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_tsearch_refuses_before_queries_on_publication_child_and_top_errors);
	UT_RUN(test_native_tsearch_context_guards_keep_nonshared_routes);
	UT_RUN(test_native_tsearch_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_tsearch_retains_parser_template_permission_refusals);
	UT_RUN(test_native_publication_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_publication_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_publication_context_guards_keep_nonshared_route);
	UT_RUN(test_native_publication_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_database_settings_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_database_settings_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_database_settings_context_guards_keep_nonshared_route);
	UT_RUN(test_native_database_settings_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_view_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_view_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_view_context_guards_keep_nonshared_route);
	UT_RUN(test_native_view_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_rule_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_rule_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_rule_context_guards_keep_nonshared_route);
	UT_RUN(test_native_rule_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_sequence_alter_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_sequence_alter_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_sequence_alter_context_guards_keep_nonshared_route);
	UT_RUN(test_native_sequence_alter_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_role_alter_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_role_alter_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_role_alter_context_guards_keep_nonshared_route);
	UT_RUN(test_native_role_alter_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_role_settings_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_role_settings_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_role_settings_context_guards_keep_nonshared_route);
	UT_RUN(test_native_role_settings_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_role_settings_keeps_global_permission_refusal_after_binding);
	UT_RUN(test_native_role_drop_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_role_drop_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_role_drop_context_guards_keep_nonshared_route);
	UT_RUN(test_native_role_drop_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_role_drop_keeps_permission_refusal_after_binding);
	UT_RUN(test_native_role_grant_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_role_grant_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_role_grant_context_guards_keep_nonshared_route);
	UT_RUN(test_native_role_grant_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_role_create_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_role_create_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_role_create_context_guards_keep_nonshared_route);
	UT_RUN(test_native_role_create_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_role_create_keeps_permission_refusal_after_binding);
	UT_RUN(test_native_reassign_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_reassign_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_reassign_context_guards_keep_nonshared_route);
	UT_RUN(test_native_reassign_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_role_rename_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_role_rename_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_role_rename_context_guards_keep_nonshared_route);
	UT_RUN(test_native_role_rename_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_statistics_create_binds_before_relation_lock_in_all_profiles);
	UT_RUN(test_native_statistics_create_refuses_before_lock_on_publication_child_and_top_errors);
	UT_RUN(test_native_statistics_create_context_guards_keep_nonshared_route);
	UT_RUN(test_native_statistics_create_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_statistics_create_unsupported_from_refuses_before_allocation);
	UT_RUN(test_native_depends_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_depends_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_depends_context_guards_keep_nonshared_route);
	UT_RUN(test_native_depends_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_depends_scope_refusals_match_utility_before_binding);
	UT_RUN(test_native_acl_grant_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_acl_grant_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_acl_grant_context_guards_keep_nonshared_route);
	UT_RUN(test_native_acl_grant_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_acl_grant_keeps_original_grantor_mismatch_refusal);
	UT_RUN(test_native_default_acl_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_default_acl_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_default_acl_context_guards_keep_nonshared_route);
	UT_RUN(test_native_default_acl_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_default_acl_keeps_original_option_errors);
	UT_RUN(test_native_default_acl_keeps_original_privilege_errors);
	UT_RUN(test_native_rename_dispatch_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_rename_dispatch_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_rename_dispatch_context_guards_keep_nonshared_route);
	UT_RUN(test_native_rename_dispatch_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_rename_dispatch_scope_refusals_match_utility_before_binding);
	UT_RUN(test_native_rename_dispatch_keeps_original_unrecognized_target_error);
	UT_RUN(test_native_schema_dispatch_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_schema_dispatch_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_schema_dispatch_context_guards_keep_nonshared_route);
	UT_RUN(test_native_schema_dispatch_utility_warm_guards_preserve_binding);
	UT_RUN(test_native_schema_dispatch_scope_refusals_match_utility_before_binding);
	UT_RUN(test_native_schema_dispatch_keeps_original_unrecognized_target_error);
	UT_RUN(test_native_database_identity_binds_before_first_query_in_all_profiles);
	UT_RUN(test_native_database_identity_refuses_before_lookup_on_publication_child_and_top_errors);
	UT_RUN(test_native_database_identity_context_guards_keep_nonshared_route);
	UT_RUN(test_native_database_identity_utility_warm_guards_preserve_binding);
	UT_DONE();
	return ut_failed_count != 0;
}
