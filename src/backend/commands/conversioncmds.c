/*-------------------------------------------------------------------------
 *
 * conversioncmds.c
 *	  conversion creation command support code
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/conversioncmds.c
 *
 * PGRAC MODIFICATIONS
 *    Preflight native conversion creation before namespace catalog work.
 *    Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_mode.h"
#include "cluster/cluster_tt_local.h"
#endif
#include "commands/alter.h"
#include "commands/conversioncmds.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#ifdef USE_PGRAC_CLUSTER
#include "tcop/utility.h"
#endif
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * CREATE CONVERSION
 */
ObjectAddress
CreateConversionCommand(CreateConversionStmt *stmt)
{
	Oid			namespaceId;
	char	   *conversion_name;
	AclResult	aclresult;
	int			from_encoding;
	int			to_encoding;
	Oid			funcoid;
	const char *from_encoding_name = stmt->for_encoding_name;
	const char *to_encoding_name = stmt->to_encoding_name;
	List	   *func_name = stmt->func_name;
	static const Oid funcargs[] = {INT4OID, INT4OID, CSTRINGOID, INTERNALOID, INT4OID, BOOLOID};
	char		result[1];
	Datum		funcresult;

#ifdef USE_PGRAC_CLUSTER
	/* Direct callers must bind before namespace lookup or conversion callbacks. */
	if (cluster_shared_catalog && cluster_storage_mode_enabled()) {
		ClusterCanonicalTxnBinding binding;
		TransactionId current_xid;

		PreventCommandIfReadOnly("CREATE CONVERSION");
		PreventCommandIfParallelMode("CREATE CONVERSION");
		PreventCommandDuringRecovery("CREATE CONVERSION");
		if (IsSubTransaction())
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("shared catalog creation does not support subtransactions")));
		current_xid = GetCurrentTransactionIdIfAny();
		if (TransactionIdIsValid(current_xid)
			&& !TransactionIdEquals(current_xid, GetTopTransactionIdIfAny()))
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("shared catalog top-level transaction identity mismatch")));
		if (!cluster_tt_local_prepare_canonical_active(GetTopTransactionId(), &binding))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("canonical ACTIVE publication failed before shared catalog creation")));
	}
#endif

	/* Convert list of names to a name and namespace */
	namespaceId = QualifiedNameGetCreationNamespace(stmt->conversion_name,
													&conversion_name);

	/* Check we have creation rights in target namespace */
	aclresult = object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_SCHEMA,
					   get_namespace_name(namespaceId));

	/* Check the encoding names */
	from_encoding = pg_char_to_encoding(from_encoding_name);
	if (from_encoding < 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("source encoding \"%s\" does not exist",
						from_encoding_name)));

	to_encoding = pg_char_to_encoding(to_encoding_name);
	if (to_encoding < 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("destination encoding \"%s\" does not exist",
						to_encoding_name)));

	/*
	 * We consider conversions to or from SQL_ASCII to be meaningless.  (If
	 * you wish to change this, note that pg_do_encoding_conversion() and its
	 * sister functions have hard-wired fast paths for any conversion in which
	 * the source or target encoding is SQL_ASCII, so that an encoding
	 * conversion function declared for such a case will never be used.)
	 */
	if (from_encoding == PG_SQL_ASCII || to_encoding == PG_SQL_ASCII)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("encoding conversion to or from \"SQL_ASCII\" is not supported")));

	/*
	 * Check the existence of the conversion function. Function name could be
	 * a qualified name.
	 */
	funcoid = LookupFuncName(func_name, sizeof(funcargs) / sizeof(Oid),
							 funcargs, false);

	/* Check it returns int4, else it's probably the wrong function */
	if (get_func_rettype(funcoid) != INT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("encoding conversion function %s must return type %s",
						NameListToString(func_name), "integer")));

	/* Check we have EXECUTE rights for the function */
	aclresult = object_aclcheck(ProcedureRelationId, funcoid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_FUNCTION,
					   NameListToString(func_name));

	/*
	 * Check that the conversion function is suitable for the requested source
	 * and target encodings. We do that by calling the function with an empty
	 * string; the conversion function should throw an error if it can't
	 * perform the requested conversion.
	 */
	funcresult = OidFunctionCall6(funcoid,
								  Int32GetDatum(from_encoding),
								  Int32GetDatum(to_encoding),
								  CStringGetDatum(""),
								  CStringGetDatum(result),
								  Int32GetDatum(0),
								  BoolGetDatum(false));

	/*
	 * The function should return 0 for empty input. Might as well check that,
	 * too.
	 */
	if (DatumGetInt32(funcresult) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("encoding conversion function %s returned incorrect result for empty input",
						NameListToString(func_name))));

	/*
	 * All seem ok, go ahead (possible failure would be a duplicate conversion
	 * name)
	 */
	return ConversionCreate(conversion_name, namespaceId, GetUserId(),
							from_encoding, to_encoding, funcoid, stmt->def);
}
