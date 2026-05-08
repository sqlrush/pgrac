/*-------------------------------------------------------------------------
 *
 * cluster_ic_msg_types_srf.c
 *	  pgrac cluster IC message-type catalog SRF (spec-2.3 D8).
 *
 *	  Implements `cluster_get_ic_msg_types()` -- the SRF backing
 *	  the `pg_cluster_ic_msg_types` view (system_views.sql).  The
 *	  SRF lives in its own file so cluster_unit standalone tests
 *	  (which link cluster_ic_router.o without PG runtime) can
 *	  remain free of unresolved symbols (InitMaterializedSRF,
 *	  cstring_to_text, tuplestore_putvalues).  Mirrors the
 *	  spec-2.2 D9 split where cluster_get_ic_peers lives in
 *	  cluster_ic_tier1.c (not cluster_ic.c).
 *
 *	  Volatility:STABLE.  The dispatch_table is process-local and
 *	  populated at postmaster phase 1; from a backend's
 *	  perspective it does not change within a single statement.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ic_msg_types_srf.c
 *
 * NOTES
 *	  pgrac-original file.  Built only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER); the disable-mode stub for
 *	  `cluster_get_ic_msg_types` lives in cluster_ic.c.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/htup_details.h"
#include "funcapi.h"
#include "utils/builtins.h"

#include "cluster/cluster_ic_router.h"

#define CLUSTER_GET_IC_MSG_TYPES_NCOLS 5

/*
 * NB: PG_FUNCTION_INFO_V1(cluster_get_ic_msg_types) is emitted in
 * cluster_ic.c (always-linked file), so pg_proc.dat resolves the
 * symbol in both --enable-cluster and --disable-cluster builds.
 */
Datum
cluster_get_ic_msg_types(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	int msg_type;

	InitMaterializedSRF(fcinfo, 0);

	for (msg_type = 1; msg_type < CLUSTER_IC_MSG_TYPE_MAX; msg_type++) {
		const ClusterICMsgTypeInfo *info;
		Datum values[CLUSTER_GET_IC_MSG_TYPES_NCOLS];
		bool nulls[CLUSTER_GET_IC_MSG_TYPES_NCOLS];

		info = cluster_ic_get_msg_type_info((uint8)msg_type);
		if (info == NULL)
			continue; /* unregistered slot */

		memset(nulls, 0, sizeof(nulls));
		values[0] = Int32GetDatum((int32)info->msg_type);
		values[1] = CStringGetTextDatum(info->name);
		values[2] = Int64GetDatum((int64)info->allowed_producer_mask);
		values[3] = BoolGetDatum(info->broadcast_ok);
		values[4] = BoolGetDatum(info->handler != NULL);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum)0;
}

#endif /* USE_PGRAC_CLUSTER */
