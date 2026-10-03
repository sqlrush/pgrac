/*-------------------------------------------------------------------------
 *
 * cluster_node_remove_views.c
 *	  pgrac online node leave: fence + cluster-wide cleanup (spec-5.18) —
 *	  catalog-facing SQL.
 *
 *	  The operator entry point + observability SRF for permanent node removal
 *	  (D15 / D16):
 *	    - cluster_get_node_removal_state(): the always-1-row removal-progress SRF
 *	      backing the pg_cluster_node_removal_state view (phase / target_node_id /
 *	      coordinator_node_id / remove_epoch / fence_armed / membership_shrunk /
 *	      grd_cleaned / pcm_cleaned / ack_count / deadline + lifetime counters).
 *	      read-only -> public (the view GRANTs SELECT).
 *	    - pg_cluster_remove_node(int): the operator UDF that permanently removes a
 *	      declared node.  Mutating -> superuser-only (the C superuser() gate +
 *	      REVOKE EXECUTE FROM PUBLIC in system_views.sql, L7).  Returns a text
 *	      status from the behaviour matrix: accepted / noop:already_removed /
 *	      resume:cleanup_pending / rejected:<reason>.
 *
 *	  Kept separate from the runtime cluster_node_remove.c so this file can be
 *	  linked unconditionally (the pg_proc.dat entries are unconditional, so the
 *	  fmgr symbols must resolve in --disable-cluster builds too); the real bodies
 *	  are #ifdef USE_PGRAC_CLUSTER and the disable-mode stubs raise
 *	  ERRCODE_FEATURE_NOT_SUPPORTED — the same pattern as cluster_clean_leave_views.c.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_node_remove_views.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linked in both build modes (SRF/UDF
 *	  symbols referenced unconditionally by pg_proc.dat); the bodies are
 *	  --enable-cluster only.
 *	  Spec: spec-5.18-online-node-leave-fence-cleanup.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "utils/fmgrprotos.h"
#include "utils/jsonb.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"		/* superuser() */
#include "utils/builtins.h" /* cstring_to_text */

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h" /* cluster_enabled */
#include "cluster/cluster_node_remove.h"
#include "cluster/cluster_membership.h"
#endif

PG_FUNCTION_INFO_V1(cluster_get_node_removal_state);
PG_FUNCTION_INFO_V1(pg_cluster_remove_node);

/* Exact administrative identities are distinct from the legacy target-only
 * commands. A response never grants admission or releases an operation.
 * Author: SqlRush <sqlrush@gmail.com> */
PG_FUNCTION_INFO_V1(pg_cluster_membership_command);

#ifdef USE_PGRAC_CLUSTER
static JsonbValue *
membership_command_field(Jsonb *request, const char *name, enum jbvType type)
{
	JsonbValue key;
	JsonbValue *value;

	key.type = jbvString;
	key.val.string.val = (char *)name;
	key.val.string.len = strlen(name);
	value = findJsonbValueFromContainer(&request->root, JB_FOBJECT, &key);
	if (value == NULL || value->type != type)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid membership request field \"%s\"", name),
				 errhint("Provide field \"%s\" as %s; run membership commands as a superuser.",
						 name, type == jbvNumeric ? "a JSON unsigned integer" : "a JSON string")));
	return value;
}

static uint64
membership_command_uint64(Jsonb *request, const char *name)
{
	JsonbValue *value = membership_command_field(request, name, jbvNumeric);
	char *decimal
		= DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(value->val.numeric)));
	const char *p;
	uint64 result = 0;

	/* Numeric output is decimal text; do not round through float8 or int8. */
	for (p = decimal; *p != '\0'; p++) {
		if (*p < '0' || *p > '9' || result > (UINT64_MAX - (*p - '0')) / 10)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("invalid unsigned membership field \"%s\"", name),
							errhint("Use a JSON integer from 0 through 18446744073709551615 "
									"without quotes or a fractional part.")));
		result = result * 10 + (*p - '0');
	}
	pfree(decimal);
	return result;
}

static bool
membership_command_string(JsonbValue *value, const char *expected)
{
	return value->val.string.len == strlen(expected)
		   && memcmp(value->val.string.val, expected, value->val.string.len) == 0;
}

static void
membership_command_validate(Jsonb *request)
{
	ClusterMembershipRequest key = { 0 };
	JsonbValue *value;
	uint64 target;
	int i, digit = 0;

	if (!JB_ROOT_IS_OBJECT(request) || JB_ROOT_COUNT(request) != 8
		|| membership_command_uint64(request, "version") != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("exact membership request fields and version 1 are required"),
				 errhint("Provide exactly these JSON fields: version (1), operation_kind, "
						 "target_node, guest_uuid, expected_formation, operation_generation, "
						 "expected_old_incarnation, reserved_new_incarnation.")));
	value = membership_command_field(request, "operation_kind", jbvString);
	if (membership_command_string(value, "leave"))
		key.operation_kind = CLUSTER_MEMBERSHIP_LEAVE;
	else if (membership_command_string(value, "remove"))
		key.operation_kind = CLUSTER_MEMBERSHIP_REMOVE;
	else if (membership_command_string(value, "rejoin"))
		key.operation_kind = CLUSTER_MEMBERSHIP_REJOIN;
	else
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unsupported membership operation"),
						errhint("Set request operation_kind to the JSON string \"leave\", "
								"\"remove\", or \"rejoin\".")));
	target = membership_command_uint64(request, "target_node");
	/* The administrative protocol uses the current common 16-node limit. */
	if (target >= 16)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("membership target node is outside the supported range"),
						errhint("Set target_node to a JSON integer from 0 through 15.")));
	key.target_node = (int32)target;
	key.expected_formation = membership_command_uint64(request, "expected_formation");
	key.operation_generation = membership_command_uint64(request, "operation_generation");
	key.expected_old_incarnation = membership_command_uint64(request, "expected_old_incarnation");
	key.reserved_new_incarnation = membership_command_uint64(request, "reserved_new_incarnation");
	value = membership_command_field(request, "guest_uuid", jbvString);
	if (value->val.string.len != 36)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("canonical nonzero guest UUID is required"),
				 errhint("Use a nonzero lowercase UUID string in 8-4-4-4-12 hexadecimal format.")));
	for (i = 0; i < 36; i++) {
		char c = value->val.string.val[i];
		int nibble;

		if (i == 8 || i == 13 || i == 18 || i == 23) {
			if (c == '-')
				continue;
		} else if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			nibble = c <= '9' ? c - '0' : c - 'a' + 10;
			key.guest_uuid[digit / 2] |= nibble << (digit % 2 == 0 ? 4 : 0);
			digit++;
			continue;
		}
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("canonical nonzero guest UUID is required"),
				 errhint("Use a nonzero lowercase UUID string in 8-4-4-4-12 hexadecimal format.")));
	}
	if (!cluster_membership_request_valid(&key))
		ereport(
			ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("membership request identity is invalid"),
			 errhint(
				 "Use a nonzero guest UUID and positive expected_formation, operation_generation, "
				 "and expected_old_incarnation integers. For rejoin, reserved_new_incarnation must "
				 "exceed expected_old_incarnation; for leave/remove, it must be 0.")));
}
#endif

Datum
pg_cluster_membership_command(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(
			ERROR,
			(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			 errmsg("must be superuser to issue a membership command"),
			 errhint(
				 "Connect as a database superuser; a function EXECUTE grant is insufficient.")));
#ifdef USE_PGRAC_CLUSTER
	{
		text *action;
		Jsonb *request = NULL;
		JsonbValue scalar;
		bool status;
		const char *reason;
		Datum args[10];
		Oid types[10] = { TEXTOID, INT4OID, TEXTOID,  TEXTOID, TEXTOID,
						  TEXTOID, TEXTOID, JSONBOID, TEXTOID, TEXTOID };
		bool nulls[10] = { false };

		if (PG_ARGISNULL(0))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("membership command action is required"),
					 errhint("Use action 'precheck', 'execute', or 'status' as a superuser.")));
		action = PG_GETARG_TEXT_PP(0);
		status = VARSIZE_ANY_EXHDR(action) == 6 && memcmp(VARDATA_ANY(action), "status", 6) == 0;
		if (!status
			&& !(VARSIZE_ANY_EXHDR(action) == 8 && memcmp(VARDATA_ANY(action), "precheck", 8) == 0)
			&& !(VARSIZE_ANY_EXHDR(action) == 7 && memcmp(VARDATA_ANY(action), "execute", 7) == 0))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unknown membership command action"),
					 errhint("Use action 'precheck', 'execute', or 'status' as a superuser.")));
		if (!PG_ARGISNULL(1)) {
			request = PG_GETARG_JSONB_P(1);
			if (JsonbExtractScalar(&request->root, &scalar) && scalar.type == jbvNull)
				request = NULL;
		}
		if (request != NULL)
			membership_command_validate(request);
		else if (!status)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("exact membership request is required"),
							errhint("Provide the version 1 JSON request with all eight identity "
									"fields. Only 'status' permits a null request.")));
		/* No administrative owner can publish a qualified result yet. Never
		 * submit through LMON's mailbox from a backend or infer idle locally. */
		reason = cluster_enabled ? "membership_authority_unavailable" : "cluster_disabled";
		args[0] = CStringGetTextDatum("version");
		args[1] = Int32GetDatum(1);
		args[2] = CStringGetTextDatum("action");
		args[3] = PointerGetDatum(action);
		args[4] = CStringGetTextDatum("status");
		args[5] = CStringGetTextDatum("blocked");
		args[6] = CStringGetTextDatum("request");
		args[7] = PointerGetDatum(request);
		nulls[7] = request == NULL;
		args[8] = CStringGetTextDatum("reason");
		args[9] = CStringGetTextDatum(reason);
		PG_RETURN_DATUM(jsonb_build_object_worker(10, args, nulls, types, false, true));
	}
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("membership commands require a --enable-cluster build"),
					errhint("Use a --enable-cluster server and connect as a superuser.")));
	PG_RETURN_NULL();
#endif
}


#ifdef USE_PGRAC_CLUSTER

/* count set bits in the survivor-ack bitmap (ack_count column). */
static int
nr_views_popcount(const uint8 *bmp, int nbytes)
{
	int i, n = 0;

	for (i = 0; i < nbytes; i++) {
		uint8 b = bmp[i];

		while (b) {
			n += (b & 1);
			b >>= 1;
		}
	}
	return n;
}

/*
 * cluster_get_node_removal_state -- always-1-row removal progress (D15).
 * cluster.enabled=off returns 0 rows (distinguishes "feature off" from "on, idle"
 * like pg_cluster_reconfig_state).  Idle surfaces as phase='idle',
 * target_node_id=-1, deadline_us NULL.
 */
Datum
cluster_get_node_removal_state(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ClusterNodeRemoveState st;
	Datum values[14];
	bool nulls[14];

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (!cluster_enabled)
		return (Datum)0; /* disabled — 0 rows */

	cluster_node_remove_get_state(&st); /* consistent snapshot under the LWLock */

	memset(nulls, false, sizeof(nulls));
	values[0] = PointerGetDatum(
		cstring_to_text(cluster_node_remove_phase_str((int)pg_atomic_read_u32(&st.phase))));
	values[1] = Int32GetDatum(st.target_node_id);
	values[2] = Int32GetDatum(st.coordinator_node_id);
	values[3] = Int64GetDatum((int64)st.remove_epoch);
	values[4] = BoolGetDatum(st.fence_armed);
	values[5] = BoolGetDatum(st.membership_shrunk);
	values[6] = BoolGetDatum(st.grd_cleaned);
	values[7] = BoolGetDatum(st.pcm_cleaned);
	values[8]
		= Int32GetDatum(nr_views_popcount(st.ack_bitmap, CLUSTER_NODE_REMOVE_ACK_BITMAP_BYTES));
	if (st.cleanup_deadline_us == 0)
		nulls[9] = true; /* idle / pre-cleanup — no deadline */
	else
		values[9] = Int64GetDatum((int64)st.cleanup_deadline_us);
	values[10] = Int64GetDatum((int64)pg_atomic_read_u64(&st.removal_committed_count));
	values[11] = Int64GetDatum((int64)pg_atomic_read_u64(&st.cleanup_blocked_count));
	values[12] = Int64GetDatum((int64)pg_atomic_read_u64(&st.leftover_detected_count));
	values[13] = Int64GetDatum((int64)pg_atomic_read_u64(&st.zombie_write_rejected_count));

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum)0;
}

/*
 * pg_cluster_remove_node(node_id int) -- operator entry: permanently remove a
 * declared node (D16).  Superuser-only (mutating, L7).  Maps the C driver's
 * ClusterRemoveRequestResult to the behaviour-matrix text.
 */
Datum
pg_cluster_remove_node(PG_FUNCTION_ARGS)
{
	int32 node_id = PG_GETARG_INT32(0);

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to remove a cluster node")));
	if (!cluster_enabled)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster.enabled is off; node removal is not available")));

	PG_RETURN_TEXT_P(cstring_to_text(
		cluster_node_remove_request_result_str(cluster_node_remove_request(node_id))));
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_get_node_removal_state(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_get_node_removal_state requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
pg_cluster_remove_node(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_remove_node requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
