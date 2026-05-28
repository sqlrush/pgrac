/*-------------------------------------------------------------------------
 *
 * cluster_undo_srf.c
 *	  pgrac spec-3.7 D8 — sanity reader SQL function for cluster undo
 *	  records.
 *
 *	  Provides cluster_undo_get_record(uba bytea) → bytea — own-instance
 *	  read only at spec-3.7;  cross-instance read via Cache Fusion deferred
 *	  to spec-3.9.
 *
 *	  Permission model:  pg_read_server_files role required(per spec-3.7
 *	  §3.5 + Q5=A,similar to pg_read_binary_file)。
 *
 *	  Failure modes:
 *	    - Non-superuser without pg_read_server_files → SQLSTATE 42501
 *	    - Invalid 16B UBA bytes length → NULL
 *	    - UBA decode fail / segment file missing → NULL (WARNING optional)
 *	    - Cross-instance UBA → NULL + WARNING(spec-3.9 forward-link)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.7-undo-record-format-allocator.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_authid.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "varatt.h" /* VARDATA_ANY / VARSIZE_ANY_EXHDR / SET_VARSIZE */

#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_undo_record_api.h"


/*
 * cluster_undo_get_record(uba bytea) → bytea
 *
 *	Read one undo record by 16-byte UBA.  Returns record bytes
 *	(UndoRecordHeader 64B + payload) on success;  NULL on failure.
 *
 *	Permission:  pg_read_server_files role required.
 */
PG_FUNCTION_INFO_V1(cluster_undo_get_record_srf);

Datum
cluster_undo_get_record_srf(PG_FUNCTION_ARGS)
{
	bytea *uba_bytea;
	int uba_len;
	UBA uba;
	char record_buf[BLCKSZ];
	size_t record_len;
	bytea *result;

	/* Permission check:  pg_read_server_files (member of role) only. */
	if (!has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES))
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied to read cluster undo record"),
						errhint("Only members of the pg_read_server_files role "
								"may read cluster undo records.")));

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	uba_bytea = PG_GETARG_BYTEA_PP(0);
	uba_len = VARSIZE_ANY_EXHDR(uba_bytea);

	/* UBA must be exactly 16 bytes per spec-3.4b. */
	if (uba_len != (int)sizeof(UBA))
		PG_RETURN_NULL();

	memcpy(&uba, VARDATA_ANY(uba_bytea), sizeof(UBA));

	/* InvalidUba sentinel (all zero) → NULL. */
	if (UBA_is_invalid(uba))
		PG_RETURN_NULL();

	/* Read the undo record into local buffer. */
	record_len = cluster_undo_get_record(uba, record_buf, sizeof(record_buf));

	if (record_len == 0)
		PG_RETURN_NULL();

	/* Build bytea result. */
	result = (bytea *)palloc(VARHDRSZ + record_len);
	SET_VARSIZE(result, VARHDRSZ + record_len);
	memcpy(VARDATA(result), record_buf, record_len);

	PG_RETURN_BYTEA_P(result);
}
